
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/main/database.hpp"

#include "s2/s2cell_union.h"
#include "s2/s2region_coverer.h"

#include "s2_endian.hpp"
#include "s2_types.hpp"

#include "arrow_scalar_udf.hpp"
#include "function_builder.hpp"
#include "s2geography/coverings.h"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2Covering {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_covering", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(Types::S2_CELL_UNION());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Returns the S2 cell covering of the geography.

A covering is a deterministic S2_CELL_UNION (i.e., list of S2_CELLs) that
completely covers a geography. This is useful as a compact approximation
of a geography that can be used to select possible candidates for intersection.

Note that an S2_CELL_UNION is a thin wrapper around a LIST of S2_CELL, such
that DuckDB LIST functions can be used to unnest, extract, or otherwise
interact with the result.

See the [Cell Operators](#cellops) section for ways to interact with cells.
)");
          func.SetExample(R"(
SELECT s2_covering(s2_data_country('Germany')) AS covering;
----
-- Find countries that might contain Berlin
SELECT name as country, cell FROM (
  SELECT name, UNNEST(s2_covering(geog)) as cell
  FROM s2_data_countries()
) WHERE
s2_cell_contains(cell, s2_data_city('Berlin')::S2_CELL_CENTER::S2_CELL);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_covering_fixed_level", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.AddParameter("fixed_level", LogicalType::INTEGER);
            variant.SetReturnType(Types::S2_CELL_UNION());
            variant.SetFunction(ExecuteFnFixedLevel);
          });

          func.SetDescription(
              R"(
Returns the S2 cell covering of the geography with a fixed level.

See `[s2_covering](#s2_covering)` for further detail and examples.
)");
          func.SetExample(R"(
SELECT s2_covering_fixed_level(s2_data_country('Germany'), 3) AS covering;
----
SELECT s2_covering_fixed_level(s2_data_country('Germany'), 4) AS covering;
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::CoveringCellIdsKernel>(args, state,
                                                                        result);
  }

  static void ExecuteFnFixedLevel(DataChunk& args, ExpressionState& state,
                                  Vector& result) {
    auto& level = args.data[1];
    if (level.GetVectorType() != VectorType::CONSTANT_VECTOR) {
      throw InvalidInputException("s2_covering_fixed_level(): level must be a constant");
    }
    DataChunk input;
    input.InitializeEmpty(
        {Types::GEOGRAPHY(), LogicalType::INTEGER, LogicalType::INTEGER});
    input.data[0].Reference(args.data[0]);
    input.data[1].Reference(level);
    input.data[2].Reference(level);
    input.SetCardinalityUnsafe(args.size());
    ExecuteArrowScalar(s2geography::sedona_udf::CoveringCellIdsLevelRangeKernel,
                       state.GetContext(), input, result);
  }
};

struct S2BoundsRect {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_bounds_box", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(Types::S2_BOX());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Returns the bounds of the input geography as a box with Cartesian edges.

The output xmin may be greater than xmax if the geography crosses the
antimeridian. Empty geographies return NULL.
)");
          func.SetExample(R"(
SELECT s2_bounds_box(s2_data_country('Germany')) as rect;
----
SELECT s2_bounds_box(s2_data_country('Fiji')) as rect;
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::BoundingBoxKernel>(args, state, result);
  }
};

struct S2BoundsRectAgg {
  // Needs to be trivially everythingable, so we can't just use S2LatLngRect
  struct State {
    R1Interval lat;
    S1Interval lng;

    void Init() {
      auto rect = S2LatLngRect::Empty();
      lat = rect.lat();
      lng = rect.lng();
    }

    void Union(const S2LatLngRect& other) {
      auto rect = S2LatLngRect(lat, lng).Union(other);
      lat = rect.lat();
      lng = rect.lng();
    }

    void Union(const State& other) { Union(S2LatLngRect(other.lat, other.lng)); }
  };

  static idx_t StateSize(AggregateStateInput&) { return sizeof(State); }

  static void Initialize(AggregateStateInput&, data_ptr_t* states, idx_t count) {
    for (idx_t i = 0; i < count; i++) {
      const auto state_ptr = new (states[i]) State();
      state_ptr->Init();
    }
  }

  struct BindData : FunctionData {
    explicit BindData(ClientContext& context) : context(context) {}
    ClientContext& context;

    unique_ptr<FunctionData> Copy() const override {
      return make_uniq<BindData>(context);
    }
    bool Equals(const FunctionData& other) const override {
      return &context == &other.Cast<BindData>().context;
    }
  };

  static unique_ptr<FunctionData> Bind(BindAggregateFunctionInput& input) {
    return make_uniq<BindData>(input.GetClientContext());
  }

  static void Update(Vector inputs[], AggregateInputData& aggr, idx_t, Vector& state_vec,
                     idx_t count) {
    DataChunk args;
    args.InitializeEmpty({Types::GEOGRAPHY()});
    args.data[0].Reference(inputs[0]);
    args.SetCardinalityUnsafe(count);
    Vector bounds(Types::S2_BOX(), MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
    ExecuteArrowScalar(s2geography::sedona_udf::BoundingBoxKernel,
                       aggr.bind_data->Cast<BindData>().context, args, bounds);
    bounds.Flatten(count);

    UnifiedVectorFormat state_format;
    state_vec.ToUnifiedFormat(count, state_format);
    auto state_ptr = UnifiedVectorFormat::GetData<State*>(state_format);
    const auto& fields = StructVector::GetEntries(bounds);
    auto xmin = FlatVector::GetData<double>(fields[0]);
    auto ymin = FlatVector::GetData<double>(fields[1]);
    auto xmax = FlatVector::GetData<double>(fields[2]);
    auto ymax = FlatVector::GetData<double>(fields[3]);
    const auto& validity = FlatVector::Validity(bounds);
    for (idx_t i = 0; i < count; i++) {
      // The kernel returns NULL for empty geographies and NULL inputs.
      if (!validity.RowIsValid(i)) continue;
      S2LatLngRect rect(R1Interval(S1Angle::Degrees(ymin[i]).radians(),
                                   S1Angle::Degrees(ymax[i]).radians()),
                        S1Interval(S1Angle::Degrees(xmin[i]).radians(),
                                   S1Angle::Degrees(xmax[i]).radians()));
      state_ptr[state_format.sel->get_index(i)]->Union(rect);
    }
  }

  static void Combine(Vector& source, Vector& target, AggregateInputData& aggr_input_data,
                      idx_t count) {
    UnifiedVectorFormat source_format;
    source.ToUnifiedFormat(count, source_format);

    const auto source_ptr = UnifiedVectorFormat::GetData<State*>(source_format);
    const auto target_ptr = FlatVector::GetData<State*>(target);

    for (idx_t target_idx = 0; target_idx < count; target_idx++) {
      const auto source_idx = source_format.sel->get_index(target_idx);

      // Union the two states
      target_ptr[target_idx]->Union(*source_ptr[source_idx]);
    }
  }

  static void Finalize(Vector& state_vec, AggregateFinalizeInputData& aggr_input_data,
                       Vector& result, idx_t count, idx_t offset) {
    UnifiedVectorFormat state_format;
    state_vec.ToUnifiedFormat(count, state_format);
    const auto state_ptr = UnifiedVectorFormat::GetData<State*>(state_format);

    auto& struct_vec = StructVector::GetEntries(result);
    const auto min_x_data = FlatVector::GetDataMutable<double>(struct_vec[0]);
    const auto min_y_data = FlatVector::GetDataMutable<double>(struct_vec[1]);
    const auto max_x_data = FlatVector::GetDataMutable<double>(struct_vec[2]);
    const auto max_y_data = FlatVector::GetDataMutable<double>(struct_vec[3]);

    for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
      const auto& state = *state_ptr[state_format.sel->get_index(raw_idx)];
      const auto out_idx = raw_idx + offset;

      auto rect = S2LatLngRect(state.lat, state.lng);

      min_x_data[out_idx] = rect.lng_lo().degrees();
      min_y_data[out_idx] = rect.lat_lo().degrees();
      max_x_data[out_idx] = rect.lng_hi().degrees();
      max_y_data[out_idx] = rect.lat_hi().degrees();
    }
  }

  static void Register(ExtensionLoader& loader) {
    AggregateFunction function("s2_bounds_box_agg", {Types::GEOGRAPHY()}, Types::S2_BOX(),
                               StateSize, Initialize, Update, Combine, Finalize,
                               FunctionNullHandling::DEFAULT_NULL_HANDLING);
    function.SetBindCallback(Bind);
    function.SetFallible();
    loader.RegisterFunction(function);
  }
};

struct S2BoxLngLatAsWkb {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_box_wkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("box", Types::S2_BOX());
            variant.SetReturnType(LogicalType::BLOB);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Serialize a S2_BOX as WKB for export.
)");
          func.SetExample(R"(
SELECT s2_box_wkb(s2_bounds_box('POINT (0 1)'::GEOGRAPHY)) as rect;
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
    using GEOGRAPHY_TYPE = PrimitiveType<string_t>;

    // We need two WKB outputs: one for a normal box and one for a box that wraps
    // over the antimeridian.
    Encoder encoder;
    encoder.Ensure(92 + 1);
    encoder.put8(0x01);
    encoder.put32(3);
    encoder.put32(1);
    encoder.put32(5);
    size_t encoder_coord_offset = encoder.length();
    for (int i = 0; i < 10; i++) {
      encoder.put64(0);
    }
    char* coords = const_cast<char*>(encoder.base() + encoder_coord_offset);

    Encoder multi_encoder;
    multi_encoder.Ensure(93 * 2 + 8 + 1);
    multi_encoder.put8(0x01);
    multi_encoder.put32(6);
    multi_encoder.put32(2);
    multi_encoder.put8(0x01);
    multi_encoder.put32(3);
    multi_encoder.put32(1);
    multi_encoder.put32(5);
    size_t multi_encoder_coord_offset_east = multi_encoder.length();
    for (int i = 0; i < 10; i++) {
      multi_encoder.put64(0);
    }

    multi_encoder.put8(0x01);
    multi_encoder.put32(3);
    multi_encoder.put32(1);
    multi_encoder.put32(5);
    size_t multi_encoder_coord_offset_west = multi_encoder.length();
    for (int i = 0; i < 10; i++) {
      multi_encoder.put64(0);
    }
    char* multi_coords_east =
        const_cast<char*>(multi_encoder.base() + multi_encoder_coord_offset_east);
    char* multi_coords_west =
        const_cast<char*>(multi_encoder.base() + multi_encoder_coord_offset_west);

    auto count = args.size();
    auto& source = args.data[0];
    GenericExecutor::ExecuteUnary<BOX_TYPE, GEOGRAPHY_TYPE>(
        source, result, count, [&](BOX_TYPE& box) {
          auto xmin = box.a_val;
          auto ymin = box.b_val;
          auto xmax = box.c_val;
          auto ymax = box.d_val;
          if (xmax >= xmin) {
            PopulateCoordsFromValues(coords, xmin, ymin, xmax, ymax);
            return StringVector::AddStringOrBlob(
                result, string_t(encoder.base(), encoder.length()));
          } else {
            PopulateCoordsFromValues(multi_coords_east, xmin, ymin, 180, ymax);
            PopulateCoordsFromValues(multi_coords_west, -180, ymin, xmax, ymax);
            return StringVector::AddStringOrBlob(
                result, string_t(multi_encoder.base(), multi_encoder.length()));
          }
        });
  }

  static void PopulateCoordsFromValues(char* coords, double xmin, double ymin,
                                       double xmax, double ymax) {
    StoreDoubleLE(coords + 0 * sizeof(double), xmin);
    StoreDoubleLE(coords + 1 * sizeof(double), ymin);
    StoreDoubleLE(coords + 2 * sizeof(double), xmax);
    StoreDoubleLE(coords + 3 * sizeof(double), ymin);
    StoreDoubleLE(coords + 4 * sizeof(double), xmax);
    StoreDoubleLE(coords + 5 * sizeof(double), ymax);
    StoreDoubleLE(coords + 6 * sizeof(double), xmin);
    StoreDoubleLE(coords + 7 * sizeof(double), ymax);
    StoreDoubleLE(coords + 8 * sizeof(double), xmin);
    StoreDoubleLE(coords + 9 * sizeof(double), ymin);
  }
};

struct S2BoxStruct {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_box_struct", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("box", Types::S2_BOX());
            variant.SetReturnType(LogicalType::STRUCT({{"xmin", LogicalType::DOUBLE},
                                                       {"ymin", LogicalType::DOUBLE},
                                                       {"xmax", LogicalType::DOUBLE},
                                                       {"ymax", LogicalType::DOUBLE}}));
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Return a S2_BOX storage as a struct(xmin, ymin, xmax, ymax).
)");
          func.SetExample(R"(
SELECT s2_box_struct(s2_bounds_box('POINT (0 1)'::GEOGRAPHY)) as rect;
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    auto& struct_vec_src = StructVector::GetEntries(args.data[0]);
    auto& struct_vec_dst = StructVector::GetEntries(result);
    for (int i = 0; i < 4; i++) {
      struct_vec_dst[i].Reference(struct_vec_src[i]);
    }

    if (args.size() == 1) {
      result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
  }
};

struct S2Box {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(loader, "s2_box", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("west", LogicalType::DOUBLE);
        variant.AddParameter("south", LogicalType::DOUBLE);
        variant.AddParameter("east", LogicalType::DOUBLE);
        variant.AddParameter("north", LogicalType::DOUBLE);
        variant.SetReturnType(Types::S2_BOX());
        variant.SetFunction(ExecuteFn);
      });

      func.SetDescription(
          R"(
Create a S2_BOX from xmin (west), ymin (south), xmax (east), and ymax (north).

Note that any box where ymin > ymax is considered EMPTY for the purposes of
comparison.
)");
      func.SetExample(R"(
SELECT s2_box(5.989, 47.302, 15.017, 54.983) as box;
----
-- xmin (west) can be greater than xmax (east) (e.g., box for Fiji)
SELECT s2_box(177.285, -18.288, 177.285, -16.0209) as box;
          )");

      func.SetTag("ext", "geography");
      func.SetTag("category", "bounds");
    });
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    auto count = args.size();

    auto& xmin = args.data[0];
    auto& ymin = args.data[1];
    auto& xmax = args.data[2];
    auto& ymax = args.data[3];

    xmin.Flatten(count);
    ymin.Flatten(count);
    xmax.Flatten(count);
    ymax.Flatten(count);

    auto& children = StructVector::GetEntries(result);
    auto& xmin_child = children[0];
    auto& ymin_child = children[1];
    auto& xmax_child = children[2];
    auto& ymax_child = children[3];

    xmin_child.Reference(xmin);
    ymin_child.Reference(ymin);
    xmax_child.Reference(xmax);
    ymax_child.Reference(ymax);

    if (count == 1) {
      result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
  }
};

struct S2BoxIntersects {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_box_intersects", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("box1", Types::S2_BOX());
            variant.AddParameter("box2", Types::S2_BOX());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Return true if two boxes have any points in common.
)");
          func.SetExample(R"(
SELECT s2_box_intersects(
  s2_bounds_box(s2_data_country('Germany')),
  s2_bounds_box(s2_data_country('France'))
);
----
SELECT s2_box_intersects(
  s2_bounds_box(s2_data_country('Germany')),
  s2_bounds_box(s2_data_country('Canada'))
);
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
    using BOOL_TYPE = PrimitiveType<bool>;
    Vector& lhs_vec = args.data[0];
    Vector& rhs_vec = args.data[1];
    idx_t count = args.size();

    GenericExecutor::ExecuteBinary<BOX_TYPE, BOX_TYPE, BOOL_TYPE>(
        lhs_vec, rhs_vec, result, count, [&](BOX_TYPE& lhs, BOX_TYPE& rhs) {
          S2LatLngRect lhs_rect(S2LatLng::FromDegrees(lhs.b_val, lhs.a_val),
                                S2LatLng::FromDegrees(lhs.d_val, lhs.c_val));
          S2LatLngRect rhs_rect(S2LatLng::FromDegrees(rhs.b_val, rhs.a_val),
                                S2LatLng::FromDegrees(rhs.d_val, rhs.c_val));
          return lhs_rect.Intersects(rhs_rect);
        });
  }
};

struct S2BoxUnion {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_box_union", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("box1", Types::S2_BOX());
            variant.AddParameter("box2", Types::S2_BOX());
            variant.SetReturnType(Types::S2_BOX());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Return the smallest possible box that contains both input boxes.
)");
          func.SetExample(R"(
SELECT s2_box_union(
  s2_bounds_box(s2_data_country('Germany')),
  s2_bounds_box(s2_data_country('France'))
);
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
    Vector& lhs_vec = args.data[0];
    Vector& rhs_vec = args.data[1];
    idx_t count = args.size();

    GenericExecutor::ExecuteBinary<BOX_TYPE, BOX_TYPE, BOX_TYPE>(
        lhs_vec, rhs_vec, result, count, [&](BOX_TYPE& lhs, BOX_TYPE& rhs) {
          S2LatLngRect lhs_rect(S2LatLng::FromDegrees(lhs.b_val, lhs.a_val),
                                S2LatLng::FromDegrees(lhs.d_val, lhs.c_val));
          S2LatLngRect rhs_rect(S2LatLng::FromDegrees(rhs.b_val, rhs.a_val),
                                S2LatLng::FromDegrees(rhs.d_val, rhs.c_val));
          S2LatLngRect out = lhs_rect.Union(rhs_rect);
          return BOX_TYPE{out.lng_lo().degrees(), out.lat_lo().degrees(),
                          out.lng_hi().degrees(), out.lat_hi().degrees()};
        });
  }
};

}  // namespace

void RegisterS2GeographyBounds(ExtensionLoader& loader) {
  S2Covering::Register(loader);
  S2BoundsRect::Register(loader);
  S2BoxLngLatAsWkb::Register(loader);
  S2BoxStruct::Register(loader);
  S2Box::Register(loader);
  S2BoxIntersects::Register(loader);
  S2BoxUnion::Register(loader);

  S2BoundsRectAgg::Register(loader);
}

}  // namespace duckdb_s2
}  // namespace duckdb
