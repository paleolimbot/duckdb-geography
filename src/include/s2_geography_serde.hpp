
#include "duckdb.hpp"

#include "s2geography/geography.h"

namespace duckdb {

namespace duckdb_s2 {

class GeographyDecoder {
 public:
  s2geography::EncodeTag tag{};
  std::vector<S2CellId> covering{};

  GeographyDecoder() = default;

  void DecodeTag(string_t data) {
    decoder_.reset(data.GetPrefix(), 4);
    tag.Decode(&decoder_);
  }

  void DecodeTagAndCovering(string_t data) {
    decoder_.reset(data.GetData(), data.GetSize());
    covering.clear();
    tag.Decode(&decoder_);
    tag.DecodeCovering(&decoder_, &covering);
  }

  std::unique_ptr<s2geography::Geography> Decode(string_t data) {
    decoder_.reset(data.GetData(), data.GetSize());
    return s2geography::Geography::DecodeTagged(&decoder_);
  }

 private:
  Decoder decoder_{};
};

class GeographyEncoder {
 public:
  GeographyEncoder() {
    options_.set_coding_hint(s2coding::CodingHint::COMPACT);
    options_.set_enable_lazy_decode(true);
    options_.set_include_covering(true);
  }

  string_t Encode(const s2geography::Geography& geog) {
    encoder_.Resize(0);
    geog.EncodeTagged(&encoder_, options_);
    return string_t{encoder_.base(), static_cast<uint32_t>(encoder_.length())};
  }

  const s2geography::EncodeOptions& options() { return options_; }

 private:
  Encoder encoder_{};
  s2geography::EncodeOptions options_{};
};

}  // namespace duckdb_s2

}  // namespace duckdb
