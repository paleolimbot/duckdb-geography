#include "arrow_scalar_udf.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "nanoarrow/nanoarrow.h"
#include "s2_types.hpp"

namespace duckdb {
namespace duckdb_s2 {
namespace {

struct KernelHolder {
  SedonaCScalarKernel kernel{};
  SedonaCScalarKernelImpl impl{};

  ~KernelHolder() {
    if (impl.release) impl.release(&impl);
    if (kernel.release) kernel.release(&kernel);
  }

  void Check(int code) {
    if (code != 0) {
      throw InvalidInputException("Arrow scalar UDF %s: %s",
                                  kernel.function_name(&kernel),
                                  impl.get_last_error(&impl));
    }
  }
};

LogicalType StorageType(const LogicalType& type) {
  switch (type.id()) {
    case LogicalTypeId::LIST:
      return LogicalType::LIST(StorageType(ListType::GetChildType(type)));
    case LogicalTypeId::STRUCT: {
      child_list_t<LogicalType> children;
      for (const auto& child : StructType::GetChildTypes(type)) {
        children.emplace_back(child.first, StorageType(child.second));
      }
      return LogicalType::STRUCT(children);
    }
    default:
      // The kernels used here take primitive inputs or WKB, not parameterized
      // types such as DECIMAL. Preserve type parameters when removing aliases.
      return type.HasAlias() ? type.WithAlias("") : type;
  }
}

void PrepareOutputSchema(ArrowSchema& schema, const LogicalType& target) {
  // Import storage directly: consulting the global geoarrow.wkb registration
  // could select GEOMETRY or invoke the legacy GEOGRAPHY conversion again.
  if (ArrowSchemaSetMetadata(&schema, nullptr) != 0) {
    throw OutOfMemoryException("Could not prepare Arrow scalar UDF output schema");
  }
  if ((target == Types::S2_CELL() || target == Types::S2_CELL_CENTER()) &&
      string(schema.format) == "l") {
    // Sedona represents unsigned S2 IDs as signed int64 bit patterns. This is
    // a reinterpretation, not a numeric cast (IDs on faces 4/5 exceed INT64_MAX).
    if (ArrowSchemaSetFormat(&schema, "L") != 0) {
      throw OutOfMemoryException("Could not prepare Arrow cell ID output schema");
    }
  }
  if (target.id() == LogicalTypeId::LIST && schema.n_children == 1) {
    PrepareOutputSchema(*schema.children[0], ListType::GetChildType(target));
  } else if (target.id() == LogicalTypeId::STRUCT &&
             schema.n_children ==
                 static_cast<int64_t>(StructType::GetChildCount(target))) {
    for (idx_t i = 0; i < StructType::GetChildCount(target); i++) {
      PrepareOutputSchema(*schema.children[i], StructType::GetChildType(target, i));
    }
  }
}

}  // namespace

void ExecuteArrowScalar(ArrowScalarKernelInit init, ClientContext& context,
                        DataChunk& args, Vector& result) {
  if (args.size() == 0) return;

  KernelHolder holder;
  init(&holder.kernel);
  holder.kernel.new_impl(&holder.kernel, &holder.impl);

  // Use standard Arrow layouts irrespective of session export preferences.
  ClientProperties options;
  options.client_context = &context;
  vector<unique_ptr<ArrowSchemaWrapper>> schemas;
  vector<unique_ptr<ArrowArrayWrapper>> arrays;
  vector<unsafe_unique_array<char>> metadata;
  vector<const ArrowSchema*> arg_types;
  vector<ArrowArray*> arg_arrays;
  for (idx_t i = 0; i < args.ColumnCount(); i++) {
    const auto& type = args.data[i].GetType();
    auto storage = StorageType(type);
    auto schema = make_uniq<ArrowSchemaWrapper>();
    ArrowConverter::ToArrowSchema(&schema->arrow_schema, {storage}, {"arg"}, options);
    auto* child_schema = schema->arrow_schema.children[0];
    if (type == Types::GEOGRAPHY()) {
      ArrowSchemaMetadata geo_metadata;
      geo_metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME, "geoarrow.wkb");
      geo_metadata.AddOption(
          ArrowSchemaMetadata::ARROW_METADATA_KEY,
          R"({"edges":"spherical","crs":"OGC:CRS84","crs_type":"authority_code"})");
      metadata.push_back(geo_metadata.SerializeMetadata());
      child_schema->metadata = metadata.back().get();
    }

    bool scalar = args.data[i].GetVectorType() == VectorType::CONSTANT_VECTOR;
    DataChunk input;
    input.InitializeEmpty({storage});
    input.data[0].Reinterpret(args.data[i]);
    input.SetCardinalityUnsafe(scalar ? 1 : args.size());
    auto array = make_uniq<ArrowArrayWrapper>();
    ArrowConverter::ToArrowArray(input, &array->arrow_array, options, {});
    auto* child_array = array->arrow_array.children[0];
    arg_types.push_back(child_schema);
    arg_arrays.push_back(child_array);
    schemas.push_back(std::move(schema));
    arrays.push_back(std::move(array));
  }

  ArrowSchemaWrapper output_schema;
  holder.Check(holder.impl.init(&holder.impl, arg_types.data(), nullptr, arg_types.size(),
                                &output_schema.arrow_schema));
  if (!output_schema.arrow_schema.release) {
    throw InvalidInputException(
        "Arrow scalar UDF %s does not support these argument types",
        holder.kernel.function_name(&holder.kernel));
  }
  // Copy into a schema owned by our nanoarrow instance before modifying it;
  // producer-owned format/metadata pointers must not be overwritten or freed.
  ArrowSchemaWrapper storage_schema;
  if (ArrowSchemaDeepCopy(&output_schema.arrow_schema, &storage_schema.arrow_schema) !=
      0) {
    throw OutOfMemoryException("Could not copy Arrow scalar UDF output schema");
  }
  PrepareOutputSchema(storage_schema.arrow_schema, result.GetType());
  auto output_type = ArrowType::GetTypeFromSchema(context, storage_schema.arrow_schema);
  output_type->ThrowIfInvalid();
  if (output_type->GetDuckType() != StorageType(result.GetType())) {
    throw InternalException("Arrow scalar UDF %s returned an unexpected type: %s",
                            holder.kernel.function_name(&holder.kernel),
                            output_type->GetDuckType().ToString());
  }

  auto output = make_shared_ptr<ArrowArrayWrapper>();
  holder.Check(holder.impl.execute(&holder.impl, arg_arrays.data(), arg_arrays.size(),
                                   args.size(), &output->arrow_array));
  auto& array = output->arrow_array;
  if (!array.release ||
      (array.length != 1 && array.length != static_cast<int64_t>(args.size()))) {
    throw InternalException("Arrow scalar UDF returned an invalid batch length");
  }

  Vector converted(result.GetType(), MaxValue<idx_t>(args.size(), STANDARD_VECTOR_SIZE));
  ArrowArrayScanState scan_state(context);
  scan_state.owned_data = output;
  ArrowToDuckDBConversion::SetValidityMask(converted, array, 0, array.length, 0, -1);
  ArrowToDuckDBConversion::ColumnArrowToDuckDB(converted, array, 0, scan_state,
                                               array.length, *output_type);
  FlatVector::SetSize(converted, array.length);
  if (array.length == 1) {
    result.Reference(converted.GetValue(0), count_t(args.size()));
  } else {
    result.Reference(converted);
  }
}

}  // namespace duckdb_s2
}  // namespace duckdb
