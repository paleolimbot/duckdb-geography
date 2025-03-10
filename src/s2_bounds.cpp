

#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell_union.h"
#include "s2/s2region_coverer.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

#include "function_builder.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2Covering {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_covering", [](ScalarFunctionBuilder& func) {
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
        instance, "s2_covering_fixed_level", [](ScalarFunctionBuilder& func) {
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

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    S2RegionCoverer coverer;
    Execute(args.data[0], result, args.size(), coverer);
  }

  static inline void ExecuteFnFixedLevel(DataChunk& args, ExpressionState& state,
                                         Vector& result) {
    Vector& max_cells_param = args.data[1];
    if (max_cells_param.GetVectorType() != VectorType::CONSTANT_VECTOR) {
      throw InvalidInputException("s2_covering_fixed_level(): level must be a constant");
    }

    int fixed_level = max_cells_param.GetValue(0).GetValue<int>();
    if (fixed_level < 0 || fixed_level > S2CellId::kMaxLevel) {
      throw InvalidInputException(
          "s2_covering_fixed_level(): level must be between 0 and 30");
    }

    S2RegionCoverer coverer;
    coverer.mutable_options()->set_fixed_level(fixed_level);
    Execute(args.data[0], result, args.size(), coverer);
  }

  static void Execute(Vector& source, Vector& result, idx_t count,
                      S2RegionCoverer& coverer) {
    ListVector::Reserve(result, count * coverer.options().max_cells());
    uint64_t offset = 0;

    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, list_entry_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return list_entry_t{0, 0};
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER: {
              decoder.DecodeTagAndCovering(geog_str);
              S2CellId cell_id =
                  decoder.covering[0].parent(coverer.options().max_level());
              ListVector::PushBack(result, Value::UBIGINT(cell_id.id()));
              list_entry_t out{offset, 1};
              offset += 1;
              return out;
            }

            default: {
              auto geog = decoder.Decode(geog_str);
              S2CellUnion covering = coverer.GetCovering(*geog->Region());
              for (const auto cell_id : covering) {
                ListVector::PushBack(result, Value::UBIGINT(cell_id.id()));
              }

              list_entry_t out{offset, covering.size()};
              offset += out.length;
              return out;
            }
          }
        });
  }
};

struct S2BoundsRect {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_bounds_box", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(Types::S2_BOX());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Returns the bounds of the input geography as a box with Cartesian edges.

The output xmin may be greater than xmax if the geography crosses the
antimeridian.
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

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    auto count = args.size();
    auto& input = args.data[0];

    using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
    using GEOGRAPHY_TYPE = PrimitiveType<string_t>;

    GeographyDecoder decoder;

    GenericExecutor::ExecuteUnary<GEOGRAPHY_TYPE, BOX_TYPE>(
        input, result, count, [&](GEOGRAPHY_TYPE& blob) {
          decoder.DecodeTag(blob.val);
          S2LatLngRect out;
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            out = S2LatLngRect::Empty();
          } else if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER) {
            uint64_t cell_id = LittleEndian::Load64(blob.val.GetData() + 4);
            S2CellId cell(cell_id);
            out = S2LatLngRect::FromPoint(cell.ToLatLng());
          } else {
            auto geog = decoder.Decode(blob.val);
            out = geog->Region()->GetRectBound();
          }
          return BOX_TYPE{out.lng_lo().degrees(), out.lat_lo().degrees(),
                          out.lng_hi().degrees(), out.lat_hi().degrees()};
        });
  }
};

// Needs to be trivially everythingable, so we can't just use S2LatLngRect
struct BoundsAggState {
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

  void Union(const BoundsAggState& other) { Union(S2LatLngRect(other.lat, other.lng)); }
};

struct S2BoundsRectAgg {
  template <class STATE>
  static void Initialize(STATE& state) {
    state.Init();
  }

  template <class STATE, class OP>
  static void Combine(const STATE& source, STATE& target, AggregateInputData&) {
    target.Union(source);
  }

  template <class INPUT_TYPE, class STATE, class OP>
  static void Operation(STATE& state, const INPUT_TYPE& input, AggregateUnaryInput&) {
    GeographyDecoder decoder;
    decoder.DecodeTag(input);
    if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
      return;
    }

    if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER) {
      uint64_t cell_id = LittleEndian::Load64(input.GetData() + 4);
      S2CellId cell(cell_id);
      S2LatLng pt = cell.ToLatLng();
      S2LatLngRect rect(pt, pt);
      state.Union(rect);
    } else {
      auto geog = decoder.Decode(input);
      S2LatLngRect rect = geog->Region()->GetRectBound();
      state.Union(rect);
    }
  }

  template <class INPUT_TYPE, class STATE, class OP>
  static void ConstantOperation(STATE& state, const INPUT_TYPE& input,
                                AggregateUnaryInput& agg, idx_t) {
    Operation<INPUT_TYPE, STATE, OP>(state, input, agg);
  }

  template <class T, class STATE>
  static void Finalize(STATE& state, T& target, AggregateFinalizeData& finalize_data) {
    auto rect = S2LatLngRect(state.lat, state.lng);

    auto& struct_vec = StructVector::GetEntries(finalize_data.result);
    auto min_x_data = FlatVector::GetData<double>(*struct_vec[0]);
    auto min_y_data = FlatVector::GetData<double>(*struct_vec[1]);
    auto max_x_data = FlatVector::GetData<double>(*struct_vec[2]);
    auto max_y_data = FlatVector::GetData<double>(*struct_vec[3]);

    idx_t i = finalize_data.result_idx;
    min_x_data[i] = rect.lng_lo().degrees();
    min_y_data[i] = rect.lat_lo().degrees();
    max_x_data[i] = rect.lng_hi().degrees();
    max_y_data[i] = rect.lat_hi().degrees();
  }

  static bool IgnoreNull() { return true; }
};

void RegisterAgg(DatabaseInstance& instance) {
  auto function = AggregateFunction::UnaryAggregate<BoundsAggState, string_t, string_t,
                                                    S2BoundsRectAgg>(Types::GEOGRAPHY(),
                                                                     Types::S2_BOX());

  // Register the function
  function.name = "s2_bounds_box_agg";
  ExtensionUtil::RegisterFunction(instance, function);
}

struct S2BoxLngLatAsWkb {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_box_wkb", [](ScalarFunctionBuilder& func) {
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
    LittleEndian::Store(xmin, coords + 0 * sizeof(double));
    LittleEndian::Store(ymin, coords + 1 * sizeof(double));
    LittleEndian::Store(xmax, coords + 2 * sizeof(double));
    LittleEndian::Store(ymin, coords + 3 * sizeof(double));
    LittleEndian::Store(xmax, coords + 4 * sizeof(double));
    LittleEndian::Store(ymax, coords + 5 * sizeof(double));
    LittleEndian::Store(xmin, coords + 6 * sizeof(double));
    LittleEndian::Store(ymax, coords + 7 * sizeof(double));
    LittleEndian::Store(xmin, coords + 8 * sizeof(double));
    LittleEndian::Store(ymin, coords + 9 * sizeof(double));
  }
};

struct S2BoxStruct {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_box_struct", [](ScalarFunctionBuilder& func) {
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
      struct_vec_dst[i]->Reference(*struct_vec_src[i]);
    }

    if (args.size() == 1) {
      result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
  }
};

struct S2Box {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(instance, "s2_box", [](ScalarFunctionBuilder& func) {
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

    xmin_child->Reference(xmin);
    ymin_child->Reference(ymin);
    xmax_child->Reference(xmax);
    ymax_child->Reference(ymax);

    if (count == 1) {
      result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
  }
};

struct S2BoxIntersects {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_box_intersects", [](ScalarFunctionBuilder& func) {
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
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_box_union", [](ScalarFunctionBuilder& func) {
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

void RegisterS2GeographyBounds(DatabaseInstance& instance) {
  S2Covering::Register(instance);
  S2BoundsRect::Register(instance);
  S2BoxLngLatAsWkb::Register(instance);
  S2BoxStruct::Register(instance);
  S2Box::Register(instance);
  S2BoxIntersects::Register(instance);
  S2BoxUnion::Register(instance);

  RegisterAgg(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
