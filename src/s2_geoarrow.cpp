#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "geoarrow/geoarrow.hpp"

#include "s2_functions_io.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct GeoArrowWKB {
  static unique_ptr<ArrowType> GetType(const ArrowSchema& schema,
                                       const ArrowSchemaMetadata& schema_metadata) {
    // Validate extension metadata. This metadata also contains a CRS, which we drop
    // because the GEOGRAPHY type does not implement a CRS at the type level.
    string extension_metadata =
        schema_metadata.GetOption(ArrowSchemaMetadata::ARROW_METADATA_KEY);
    auto data_type =
        geoarrow::GeometryDataType::Make(GEOARROW_TYPE_WKB, extension_metadata);
    if (data_type.edge_type() != GEOARROW_EDGE_TYPE_SPHERICAL) {
      throw NotImplementedException("Can't import non-spherical edges as GEOGRAPHY");
    }

    const auto format = string(schema.format);
    if (format == "z") {
      return make_uniq<ArrowType>(
          Types::GEOGRAPHY(), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
    } else if (format == "Z") {
      return make_uniq<ArrowType>(
          Types::GEOGRAPHY(),
          make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
    } else if (format == "vz") {
      return make_uniq<ArrowType>(
          Types::GEOGRAPHY(), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::VIEW));
    } else {
      throw InvalidInputException(
          "Arrow storage type \"%s\" not supported for geoarrow.wkb", format.c_str());
    }
  }

  static void PopulateSchema(DuckDBArrowSchemaHolder& root_holder, ArrowSchema& schema,
                             const LogicalType& type, ClientContext& context,
                             const ArrowTypeExtension& extension) {
    // Should really use WithCrsLonLat() here, but DuckDB itself chokes on non key/value
    // metadata https://github.com/duckdb/duckdb/issues/16321
    auto data_type = geoarrow::Wkb()
                         .WithEdgeType(GEOARROW_EDGE_TYPE_SPHERICAL)
                         .WithCrs("OGC:CRS84", GEOARROW_CRS_TYPE_AUTHORITY_CODE);

    ArrowSchemaMetadata schema_metadata;
    schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME,
                              data_type.extension_name());
    schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY,
                              data_type.extension_metadata());
    root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
    schema.metadata = root_holder.metadata_info.back().get();

    const auto options = context.GetClientProperties();
    if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
      schema.format = "Z";
    } else {
      schema.format = "z";
    }
  }

  static void ArrowToDuck(ClientContext& context, Vector& source, Vector& result,
                          idx_t count) {
    s2geography::geoarrow::ImportOptions options;
    options.set_check(false);
    options.set_oriented(true);
    ImportWKBToGeography(source, result, count);
  }

  static void DuckToArrow(ClientContext& context, Vector& source, Vector& result,
                          idx_t count) {
    ExportGeographyToWKB(source, result, count);
  }
};

void RegisterArrowExtensions(DBConfig& config) {
  config.RegisterArrowExtension(
      {"geoarrow.wkb", GeoArrowWKB::PopulateSchema, GeoArrowWKB::GetType,
       make_shared_ptr<ArrowTypeExtensionData>(Types::GEOGRAPHY(), LogicalType::BLOB,
                                               GeoArrowWKB::ArrowToDuck,
                                               GeoArrowWKB::DuckToArrow)});
}

class GeoArrowRegisterFunctionData final : public TableFunctionData {
 public:
  GeoArrowRegisterFunctionData() : finished(false) {}
  bool finished{false};
};

unique_ptr<FunctionData> GeoArrowRegisterBind(ClientContext& context,
                                              TableFunctionBindInput& input,
                                              vector<LogicalType>& return_types,
                                              vector<string>& names) {
  names.push_back("registered");
  return_types.push_back(LogicalType::BOOLEAN);
  return make_uniq<GeoArrowRegisterFunctionData>();
}

void GeoArrowRegisterScan(ClientContext& context, TableFunctionInput& data_p,
                          DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<GeoArrowRegisterFunctionData>();
  if (data.finished) {
    return;
  }

  DBConfig& config = DatabaseInstance::GetDatabase(context).config;
  if (config.HasArrowExtension(Types::GEOGRAPHY())) {
    output.SetValue(0, 0, false);
  } else {
    RegisterArrowExtensions(config);
    output.SetValue(0, 0, true);
  }

  output.SetCardinality(1);
  data.finished = true;
}
}  // namespace

void RegisterGeoArrowExtensions(DatabaseInstance& instance) {
  TableFunction register_func("s2_register_geoarrow_extensions", {}, GeoArrowRegisterScan,
                              GeoArrowRegisterBind);
  ExtensionUtil::RegisterFunction(instance, register_func);
}

}  // namespace duckdb_s2

}  // namespace duckdb
