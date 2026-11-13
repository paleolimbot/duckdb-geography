
#include "duckdb.hpp"

#include "s2geography/geography.h"
#include "s2geography/wkb.h"

namespace duckdb {

namespace duckdb_s2 {

class GeographyDecoder {
 public:
  s2geography::EncodeTag tag{};
  std::vector<S2CellId> covering{};

  GeographyDecoder() : reader_(UncheckedImportOptions()) {}

  void DecodeTag(string_t data) {
    auto geog = Decode(data);
    tag = {};
    tag.kind = geog->kind();
    if (geog->num_shapes() == 0) {
      tag.flags |= s2geography::EncodeTag::kFlagEmpty;
    }
  }

  void DecodeTagAndCovering(string_t data) {
    auto geog = Decode(data);
    tag = {};
    tag.kind = geog->kind();
    covering.clear();
    if (geog->num_shapes() == 0) {
      tag.flags |= s2geography::EncodeTag::kFlagEmpty;
    } else {
      geog->GetCellUnionBound(&covering);
      tag.covering_size = covering.size();
    }
  }

  std::unique_ptr<s2geography::Geography> Decode(string_t data) {
    return reader_.ReadFeature(
        std::string_view(data.GetData(), static_cast<size_t>(data.GetSize())));
  }

 private:
  static s2geography::geoarrow::ImportOptions UncheckedImportOptions() {
    s2geography::geoarrow::ImportOptions options;
    options.set_check(false);
    return options;
  }

  s2geography::WKBReader reader_;
};

class GeographyEncoder {
 public:
  GeographyEncoder() = default;

  string_t Encode(const s2geography::Geography& geog) {
    encoded_ = writer_.WriteFeature(geog);
    return string_t{encoded_.data(), static_cast<uint32_t>(encoded_.size())};
  }

 private:
  s2geography::WKBWriter writer_{};
  std::string encoded_{};
};

}  // namespace duckdb_s2

}  // namespace duckdb
