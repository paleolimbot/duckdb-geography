
#include "duckdb/main/database.hpp"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

#include "s2geography/build.h"
#include "s2geography_c.h"

#include "arrow_scalar_udf.hpp"
#include "function_builder.hpp"
#include "s2geography/distance.h"

namespace duckdb {

namespace duckdb_s2 {

namespace {
using UniqueGeography = std::unique_ptr<s2geography::Geography>;

class WKBOperationContext {
 public:
  explicit WKBOperationContext(int operation_id) {
    CheckCreate(S2GeogErrorCreate(&error_), "error");
    Check(S2GeogFactoryCreate(&factory_));
    Check(S2GeogCreate(&lhs_.geog));
    Check(S2GeogCreate(&rhs_.geog));
    Check(S2GeogOpCreate(&operation_, operation_id));
  }

  ~WKBOperationContext() {
    if (operation_) S2GeogOpDestroy(operation_);
    if (rhs_.geog) S2GeogDestroy(rhs_.geog);
    if (lhs_.geog) S2GeogDestroy(lhs_.geog);
    if (factory_) S2GeogFactoryDestroy(factory_);
    if (error_) S2GeogErrorDestroy(error_);
  }

  WKBOperationContext(const WKBOperationContext&) = delete;
  WKBOperationContext& operator=(const WKBOperationContext&) = delete;

  bool Execute(const string_t& lhs, const string_t& rhs) {
    Prepare(lhs_, lhs);
    Prepare(rhs_, rhs);
    Check(S2GeogOpEvalGeogGeog(operation_, lhs_.geog, rhs_.geog, error_));
    return S2GeogOpGetInt(operation_) != 0;
  }

  bool Execute(const string_t& lhs, const string_t& rhs, double arg) {
    Prepare(lhs_, lhs);
    Prepare(rhs_, rhs);
    Check(S2GeogOpEvalGeogGeogDouble(operation_, lhs_.geog, rhs_.geog, arg, error_));
    return S2GeogOpGetInt(operation_) != 0;
  }

 private:
  struct PreparedGeography {
    S2Geog* geog = nullptr;
    const char* data = nullptr;
    idx_t size = 0;
    bool initialized = false;
  };

  void Prepare(PreparedGeography& prepared, const string_t& wkb) {
    // UnifiedVectorFormat gives us stable string_t storage for this batch. A
    // constant vector therefore keeps the same data pointer across rows and is
    // parsed/prepared only once; non-constant values reuse the same geography
    // object's scratch space while retaining a zero-copy view of their WKB.
    if (prepared.initialized && prepared.data == wkb.GetData() &&
        prepared.size == wkb.GetSize()) {
      return;
    }

    Check(S2GeogFactoryInitFromWkbNonOwning(
        factory_, reinterpret_cast<const uint8_t*>(wkb.GetData()), wkb.GetSize(),
        prepared.geog, error_));
    prepared.data = wkb.GetData();
    prepared.size = wkb.GetSize();
    prepared.initialized = true;
  }

  void Check(S2GeogErrorCode code) {
    if (code != S2GEOGRAPHY_OK) {
      throw InvalidInputException("s2geography error: %s", S2GeogErrorGetMessage(error_));
    }
  }

  static void CheckCreate(S2GeogErrorCode code, const char* object_name) {
    if (code != S2GEOGRAPHY_OK) {
      throw InternalException("Could not create s2geography %s object", object_name);
    }
  }

  S2GeogError* error_ = nullptr;
  S2GeogFactory* factory_ = nullptr;
  PreparedGeography lhs_;
  PreparedGeography rhs_;
  S2GeogOp* operation_ = nullptr;
};

static void ExecuteBinaryWKBOperation(Vector& lhs, Vector& rhs, Vector& result,
                                      idx_t count, int operation_id) {
  UnifiedVectorFormat lhs_format;
  UnifiedVectorFormat rhs_format;
  lhs.ToUnifiedFormat(count, lhs_format);
  rhs.ToUnifiedFormat(count, rhs_format);

  const auto lhs_data = UnifiedVectorFormat::GetData<string_t>(lhs_format);
  const auto rhs_data = UnifiedVectorFormat::GetData<string_t>(rhs_format);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetDataMutable<bool>(result);
  auto& result_validity = FlatVector::ValidityMutable(result);
  result_validity.SetAllValid(count);

  WKBOperationContext operation(operation_id);
  for (idx_t row = 0; row < count; row++) {
    const auto lhs_index = lhs_format.sel->get_index(row);
    const auto rhs_index = rhs_format.sel->get_index(row);
    if (!lhs_format.validity.RowIsValid(lhs_index) ||
        !rhs_format.validity.RowIsValid(rhs_index)) {
      result_validity.SetInvalid(row);
      continue;
    }

    result_data[row] = operation.Execute(lhs_data[lhs_index], rhs_data[rhs_index]);
  }
}

static void ExecuteTernaryWKBOperation(Vector& lhs, Vector& rhs, Vector& arg,
                                       Vector& result, idx_t count, int operation_id) {
  UnifiedVectorFormat lhs_format;
  UnifiedVectorFormat rhs_format;
  UnifiedVectorFormat arg_format;
  lhs.ToUnifiedFormat(count, lhs_format);
  rhs.ToUnifiedFormat(count, rhs_format);
  arg.ToUnifiedFormat(count, arg_format);

  const auto lhs_data = UnifiedVectorFormat::GetData<string_t>(lhs_format);
  const auto rhs_data = UnifiedVectorFormat::GetData<string_t>(rhs_format);
  const auto arg_data = UnifiedVectorFormat::GetData<double>(arg_format);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetDataMutable<bool>(result);
  auto& result_validity = FlatVector::ValidityMutable(result);
  result_validity.SetAllValid(count);

  WKBOperationContext operation(operation_id);
  for (idx_t row = 0; row < count; row++) {
    const auto lhs_index = lhs_format.sel->get_index(row);
    const auto rhs_index = rhs_format.sel->get_index(row);
    const auto arg_index = arg_format.sel->get_index(row);
    if (!lhs_format.validity.RowIsValid(lhs_index) ||
        !rhs_format.validity.RowIsValid(rhs_index) ||
        !arg_format.validity.RowIsValid(arg_index)) {
      result_validity.SetInvalid(row);
      continue;
    }

    result_data[row] =
        operation.Execute(lhs_data[lhs_index], rhs_data[rhs_index], arg_data[arg_index]);
  }
}

struct S2BinaryIndexOp {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_mayintersect", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteMayIntersectFn);
          });

          func.SetDescription(R"(
Returns true if the two geographies may intersect.

This function uses the internal [covering](#s2_covering) stored alongside
each geography to perform a cheap check for potential intersection.
)");

          func.SetExample(R"(
-- Definitely intersects
SELECT s2_mayintersect(s2_data_country('Canada'), s2_data_city('Toronto'));
----
-- Doesn't intersect but might according to the internal coverings
SELECT s2_mayintersect(s2_data_country('Canada'), s2_data_city('Chicago'));
----
-- Definitely doesn't intersect
SELECT s2_mayintersect(s2_data_country('Canada'), s2_data_city('Berlin'));
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicates");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_intersects", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteIntersectsFn);
          });

          func.SetDescription(R"(
Returns true if the two geographies intersect.
)");

          func.SetExample(R"(
SELECT s2_intersects(s2_data_country('Canada'), s2_data_city('Toronto'));
----
SELECT s2_intersects(s2_data_country('Canada'), s2_data_city('Chicago'));
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicates");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_contains", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteContainsFn);
          });

          func.SetDescription(R"(
Returns true if the first geography contains the second.
)");

          func.SetExample(R"(
SELECT s2_contains(s2_data_country('Canada'), s2_data_city('Toronto'));
----
SELECT s2_contains(s2_data_city('Toronto'), s2_data_country('Canada'));
----
SELECT s2_contains(s2_data_country('Canada'), s2_data_city('Chicago'));
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicates");
        });

    FunctionBuilder::RegisterScalar(loader, "s2_within", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog1", Types::GEOGRAPHY());
        variant.AddParameter("geog2", Types::GEOGRAPHY());
        variant.SetReturnType(LogicalType::BOOLEAN);
        variant.SetFunction(ExecuteWithinFn);
      });

      func.SetDescription(R"(
Returns true if the first geography is within the second.
)");

      func.SetExample(R"(
SELECT s2_within(s2_data_city('Toronto'), s2_data_country('Canada'));
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "predicates");
    });

    FunctionBuilder::RegisterScalar(
        loader, "s2_disjoint", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteDisjointFn);
          });

          func.SetDescription(R"(
Returns true if the two geographies do not intersect.
)");

          func.SetExample(R"(
SELECT s2_disjoint(s2_data_city('Toronto'), s2_data_city('Vancouver'));
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicates");
        });

    FunctionBuilder::RegisterScalar(loader, "s2_equals", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog1", Types::GEOGRAPHY());
        variant.AddParameter("geog2", Types::GEOGRAPHY());
        variant.SetReturnType(LogicalType::BOOLEAN);
        variant.SetFunction(ExecuteEqualsFn);
      });

      func.SetDescription(R"(
Returns true if the two geographies are equal.

Note that this test of equality will pass for *geometrically* equal geographies
that may have the same edges but that are ordered differently.
)");
      func.SetExample(R"(
SELECT s2_equals(s2_data_country('Canada'), s2_data_country('Canada'));
----
SELECT s2_equals(s2_data_city('Toronto'), s2_data_country('Canada'));
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "predicates");
    });

    FunctionBuilder::RegisterScalar(
        loader, "s2_intersection", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteIntersectionFn);
          });

          func.SetDescription(R"(
Returns the intersection of two geographies.

Returns an empty geography if either input is empty. Empty overlay results may
retain their geometry type (for example, POINT EMPTY).
)");

          func.SetExample(R"(
SELECT s2_intersection(
  'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
  'POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))'
) as intersection
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "overlay");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_difference", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteDifferenceFn);
          });

          func.SetDescription(R"(
Returns the difference of two geographies.
)");

          func.SetExample(R"(
SELECT s2_difference(
  'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
  'POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))'
) as difference
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "overlay");
        });

    FunctionBuilder::RegisterScalar(loader, "s2_union", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog1", Types::GEOGRAPHY());
        variant.AddParameter("geog2", Types::GEOGRAPHY());
        variant.SetReturnType(Types::GEOGRAPHY());
        variant.SetFunction(ExecuteUnionFn);
      });

      func.SetDescription(R"(
Returns the union of two geographies.
)");

      func.SetExample(R"(
SELECT s2_union(
  'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
  'POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))'
) as union_
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "overlay");
    });
  }

  static void ExecuteMayIntersectFn(DataChunk& args, ExpressionState& state,
                                    Vector& result) {
    return ExecutePredicateFn(
        args, state, result,
        [](UniqueGeography lhs, UniqueGeography rhs) { return true; });
  }

  static void ExecuteIntersectsFn(DataChunk& args, ExpressionState& state,
                                  Vector& result) {
    ExecuteBinaryWKBOperation(args.data[0], args.data[1], result, args.size(),
                              S2GEOGRAPHY_OP_INTERSECTS);
  }

  static void ExecuteContainsFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ExecuteBinaryWKBOperation(args.data[0], args.data[1], result, args.size(),
                              S2GEOGRAPHY_OP_CONTAINS);
  }

  static void ExecuteWithinFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ExecuteBinaryWKBOperation(args.data[0], args.data[1], result, args.size(),
                              S2GEOGRAPHY_OP_WITHIN);
  }

  static void ExecuteDisjointFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ExecuteBinaryWKBOperation(args.data[0], args.data[1], result, args.size(),
                              S2GEOGRAPHY_OP_DISJOINT);
  }

  static void ExecuteEqualsFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ExecuteBinaryWKBOperation(args.data[0], args.data[1], result, args.size(),
                              S2GEOGRAPHY_OP_EQUALS);
  }

  template <typename Filter>
  static void ExecutePredicateFn(DataChunk& args, ExpressionState& state, Vector& result,
                                 Filter&& filter) {
    ExecutePredicate(args.data[0], args.data[1], result, args.size(), filter);
  }

  template <typename Filter>
  static void ExecutePredicate(Vector& lhs, Vector& rhs, Vector& result, idx_t count,
                               Filter&& filter) {
    GeographyDecoder lhs_decoder;
    GeographyDecoder rhs_decoder;
    std::vector<S2CellId> intersection;

    BinaryExecutor::Execute<string_t, string_t, bool>(
        lhs, rhs, result, count, [&](string_t lhs_str, string_t rhs_str) {
          lhs_decoder.DecodeTagAndCovering(lhs_str);
          if (lhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return false;
          }

          rhs_decoder.DecodeTagAndCovering(rhs_str);
          if (rhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return false;
          }

          if (!CoveringMayIntersect(lhs_decoder, rhs_decoder, &intersection)) {
            return false;
          }

          return filter(lhs_decoder.Decode(lhs_str), rhs_decoder.Decode(rhs_str));
        });
  }

  static void ExecuteIntersectionFn(DataChunk& args, ExpressionState& state,
                                    Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::IntersectionKernel>(args, state, result);
  }

  static void ExecuteDifferenceFn(DataChunk& args, ExpressionState& state,
                                  Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::DifferenceKernel>(args, state, result);
  }

  static void ExecuteUnionFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::UnionKernel>(args, state, result);
  }

  static bool CoveringMayIntersect(const GeographyDecoder& lhs,
                                   const GeographyDecoder& rhs,
                                   std::vector<S2CellId>* intersection_scratch) {
    // We don't currently omit coverings but in case we do by accident,
    // an omitted covering *might* intersect since it was just not generated.
    if (lhs.covering.empty() || rhs.covering.empty()) {
      return true;
    }

    S2CellUnion::GetIntersection(lhs.covering, rhs.covering, intersection_scratch);
    return !intersection_scratch->empty();
  }
};

struct S2DWithin {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_dwithin", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.AddParameter("distance", LogicalType::DOUBLE);
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Return true if two geographies are within a given distance (in meters).
)");
          func.SetExample(R"(
SELECT s2_dwithin(
  s2_data_city('Vancouver'),
  s2_data_country('United States of America'),
  30000
) AS is_within;
----
SELECT s2_dwithin(
  s2_data_city('Vancouver'),
  s2_data_country('United States of America'),
  40000
) AS is_within;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], args.data[1], args.data[2], result, args.size());
  }

  static void Execute(Vector& lhs, Vector& rhs, Vector& dist, Vector& result,
                      idx_t count) {
    ExecuteTernaryWKBOperation(lhs, rhs, dist, result, count,
                               S2GEOGRAPHY_OP_DISTANCE_WITHIN);
  }
};

struct S2Distance {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_distance", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::DOUBLE);
            variant.SetFunction(ExecuteDistanceFn);
          });

          func.SetDescription(R"(
Calculate the shortest distance between two geographies.

Returns NULL if either geography is empty.
)");
          func.SetExample(R"(
SELECT s2_distance(
  s2_data_city('Vancouver'),
  s2_data_country('United States of America')
) AS distance;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_max_distance", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::DOUBLE);
            variant.SetFunction(ExecuteMaxDistanceFn);
          });

          func.SetDescription(R"(
Calculate the farthest distance between two geographies.

Returns NULL if either geography is empty.
)");
          func.SetExample(R"(
SELECT s2_max_distance(
  s2_data_city('Vancouver'),
  s2_data_country('United States of America')
) AS distance;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static void ExecuteDistanceFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ExecuteArrowScalar(
        [](SedonaCScalarKernel* out) { s2geography::sedona_udf::DistanceKernel(out); },
        state.GetContext(), args, result);
  }

  static void ExecuteMaxDistanceFn(DataChunk& args, ExpressionState& state,
                                   Vector& result) {
    ExecuteArrowScalar(
        [](SedonaCScalarKernel* out) { s2geography::sedona_udf::MaxDistanceKernel(out); },
        state.GetContext(), args, result);
  }
};

}  // namespace

void RegisterS2GeographyPredicates(ExtensionLoader& loader) {
  S2BinaryIndexOp::Register(loader);
  S2Distance::Register(loader);
  S2DWithin::Register(loader);
}

}  // namespace duckdb_s2
}  // namespace duckdb
