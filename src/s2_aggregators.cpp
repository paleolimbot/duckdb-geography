
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2latlng_rect.h"

#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

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

struct ShapeUnionState {
  MutableS2ShapeIndex* shape_index;
};

struct S2UnionAgg {
  template <class STATE>
  static void Initialize(STATE& state) {
    state.shape_index = new MutableS2ShapeIndex();
  }

  template <class STATE>
  static void Destroy(STATE& state, AggregateInputData&) {
    delete state.shape_index;
  }

  template <class STATE, class OP>
  static void Combine(const STATE& source, STATE& target, AggregateInputData&) {}

  template <class INPUT_TYPE, class STATE, class OP>
  static void Operation(STATE& state, const INPUT_TYPE& input, AggregateUnaryInput&) {
    GeographyDecoder decoder;
    decoder.DecodeTag(input);
    if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
      return;
    }

    if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER) {
    } else {
      auto geog = decoder.Decode(input);
    }
  }

  template <class INPUT_TYPE, class STATE, class OP>
  static void ConstantOperation(STATE& state, const INPUT_TYPE& input,
                                AggregateUnaryInput& agg, idx_t) {
    Operation<INPUT_TYPE, STATE, OP>(state, input, agg);
  }

  template <class T, class STATE>
  static void Finalize(STATE& state, T& target, AggregateFinalizeData& finalize_data) {}

  static bool IgnoreNull() { return true; }
};

}  // namespace

void RegisterS2Aggregators(DatabaseInstance& instance) {
  auto function = AggregateFunction::UnaryAggregate<BoundsAggState, string_t, string_t,
                                                    S2BoundsRectAgg>(Types::GEOGRAPHY(),
                                                                     Types::S2_BOX());

  // Register the function
  function.name = "s2_bounds_box_agg";
  ExtensionUtil::RegisterFunction(instance, function);
}

}  // namespace duckdb_s2
}  // namespace duckdb
