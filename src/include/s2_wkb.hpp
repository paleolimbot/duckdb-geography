#pragma once

#include "duckdb.hpp"

#include "geoarrow/geoarrow.h"
#include "s2/s2loop.h"
#include "s2/s2polygon.h"
#include "s2/s2polyline.h"
#include "s2/s2validation_query.h"
#include "s2/util/coding/coder.h"

// GeoArrowGeography carries a minimal copy of these ABI declarations for
// builds that do not otherwise use GeoArrow. The full header above provides
// the same ABI plus the WKB/WKT reader and writer APIs used here.
#ifndef GEOARROW_C_ABI
#define GEOARROW_C_ABI
#endif
#include "s2geography/geoarrow-geography.h"

#include <array>

namespace duckdb {
namespace duckdb_s2 {

class WKBGeography {
 public:
  WKBGeography() {
    if (GeoArrowWKBReaderInit(&reader_) != GEOARROW_OK) {
      throw InternalException("Could not initialize GeoArrow WKB reader");
    }
  }

  ~WKBGeography() { GeoArrowWKBReaderReset(&reader_); }

  WKBGeography(const WKBGeography&) = delete;
  WKBGeography& operator=(const WKBGeography&) = delete;

  const s2geography::GeoArrowGeography& Read(const string_t& wkb) {
    GeoArrowBufferView source{reinterpret_cast<const uint8_t*>(wkb.GetData()),
                              static_cast<int64_t>(wkb.GetSize())};
    GeoArrowGeometryView view{};
    if (GeoArrowWKBReaderRead(&reader_, source, &view, &error_) != GEOARROW_OK) {
      throw InvalidInputException("Invalid WKB: %s", error_.message);
    }

    geography_.Init(view);
    return geography_;
  }

 private:
  GeoArrowWKBReader reader_{};
  GeoArrowError error_{};
  s2geography::GeoArrowGeography geography_;
};

class WKTToWKB {
 public:
  WKTToWKB() {
    if (GeoArrowWKTReaderInit(&reader_) != GEOARROW_OK) {
      throw InternalException("Could not initialize GeoArrow WKT reader");
    }
    if (GeoArrowWKBWriterInit(&writer_) != GEOARROW_OK) {
      GeoArrowWKTReaderReset(&reader_);
      throw InternalException("Could not initialize GeoArrow WKB writer");
    }
  }

  ~WKTToWKB() {
    GeoArrowWKBWriterReset(&writer_);
    GeoArrowWKTReaderReset(&reader_);
  }

  WKTToWKB(const WKTToWKB&) = delete;
  WKTToWKB& operator=(const WKTToWKB&) = delete;

  std::string Convert(const char* data, idx_t size) {
    ClosingWKBVisitor visitor(&writer_, &error_);

    GeoArrowStringView source{data, static_cast<int64_t>(size)};
    if (GeoArrowWKTReaderVisit(&reader_, source, visitor.get()) != GEOARROW_OK) {
      throw InvalidInputException("Invalid WKT: %s", error_.message);
    }

    ArrowArray array{};
    if (GeoArrowWKBWriterFinish(&writer_, &array, &error_) != GEOARROW_OK) {
      throw InvalidInputException("Could not encode WKB: %s", error_.message);
    }

    const auto offsets = static_cast<const int32_t*>(array.buffers[1]);
    const auto bytes = static_cast<const char*>(array.buffers[2]);
    std::string result(bytes + offsets[0], static_cast<idx_t>(offsets[1] - offsets[0]));
    array.release(&array);
    return result;
  }

  std::string Convert(const char* data) { return Convert(data, strlen(data)); }

 private:
  class ClosingWKBVisitor {
   public:
    ClosingWKBVisitor(GeoArrowWKBWriter* writer, GeoArrowError* error) {
      GeoArrowWKBWriterInitVisitor(writer, &sink_);
      visitor_ = {FeatStart, NullFeat, GeomStart, RingStart, Coords,
                  RingEnd,   GeomEnd,  FeatEnd,   this,      error};
      sink_.error = error;
    }

    GeoArrowVisitor* get() { return &visitor_; }

   private:
    static ClosingWKBVisitor& Self(GeoArrowVisitor* visitor) {
      return *static_cast<ClosingWKBVisitor*>(visitor->private_data);
    }

    static int FeatStart(GeoArrowVisitor* visitor) {
      auto& self = Self(visitor);
      return self.sink_.feat_start(&self.sink_);
    }

    static int NullFeat(GeoArrowVisitor* visitor) {
      auto& self = Self(visitor);
      return self.sink_.null_feat(&self.sink_);
    }

    static int GeomStart(GeoArrowVisitor* visitor, GeoArrowGeometryType geometry_type,
                         GeoArrowDimensions dimensions) {
      auto& self = Self(visitor);
      return self.sink_.geom_start(&self.sink_, geometry_type, dimensions);
    }

    static int RingStart(GeoArrowVisitor* visitor) {
      auto& self = Self(visitor);
      self.in_ring_ = true;
      self.have_coordinate_ = false;
      return self.sink_.ring_start(&self.sink_);
    }

    static int Coords(GeoArrowVisitor* visitor, const GeoArrowCoordView* coords) {
      auto& self = Self(visitor);
      if (self.in_ring_ && coords->n_coords > 0) {
        self.num_dimensions_ = coords->n_values;
        for (int dimension = 0; dimension < coords->n_values; dimension++) {
          if (!self.have_coordinate_) {
            self.first_[dimension] = GEOARROW_COORD_VIEW_VALUE(coords, 0, dimension);
          }
          self.last_[dimension] =
              GEOARROW_COORD_VIEW_VALUE(coords, coords->n_coords - 1, dimension);
        }
        self.have_coordinate_ = true;
      }
      return self.sink_.coords(&self.sink_, coords);
    }

    static int RingEnd(GeoArrowVisitor* visitor) {
      auto& self = Self(visitor);
      bool is_closed = self.have_coordinate_;
      for (int dimension = 0; dimension < self.num_dimensions_; dimension++) {
        is_closed = is_closed && self.first_[dimension] == self.last_[dimension];
      }
      if (self.have_coordinate_ && !is_closed) {
        GeoArrowCoordView closing{};
        closing.n_coords = 1;
        closing.n_values = self.num_dimensions_;
        closing.coords_stride = 1;
        for (int dimension = 0; dimension < self.num_dimensions_; dimension++) {
          closing.values[dimension] = &self.first_[dimension];
        }
        const auto result = self.sink_.coords(&self.sink_, &closing);
        if (result != GEOARROW_OK) {
          return result;
        }
      }
      self.in_ring_ = false;
      return self.sink_.ring_end(&self.sink_);
    }

    static int GeomEnd(GeoArrowVisitor* visitor) {
      auto& self = Self(visitor);
      return self.sink_.geom_end(&self.sink_);
    }

    static int FeatEnd(GeoArrowVisitor* visitor) {
      auto& self = Self(visitor);
      return self.sink_.feat_end(&self.sink_);
    }

    GeoArrowVisitor visitor_{};
    GeoArrowVisitor sink_{};
    std::array<double, 8> first_{};
    std::array<double, 8> last_{};
    int32_t num_dimensions_{};
    bool in_ring_{};
    bool have_coordinate_{};
  };

  GeoArrowWKTReader reader_{};
  GeoArrowWKBWriter writer_{};
  GeoArrowError error_{};
};

inline bool FindValidationError(const s2geography::GeoArrowGeography& geography,
                                S2Error* error) {
  bool invalid = false;
  geography.lines()->geom().VisitChains([&](s2geography::GeoArrowChain chain) {
    std::vector<S2Point> vertices;
    chain.VisitVertices([&](const S2Point& vertex) {
      vertices.push_back(vertex);
      return true;
    });

    S2Polyline line;
    line.set_s2debug_override(S2Debug::DISABLE);
    line.Init(vertices);
    invalid = line.FindValidationError(error);
    return !invalid;
  });
  if (invalid) {
    return true;
  }

  if (geography.polygons()->num_edges() == 0) {
    return false;
  }

  MutableS2ShapeIndex polygon_index;
  polygon_index.Add(
      std::make_unique<s2geography::GeoArrowLaxPolygonShape>(*geography.polygons()));
  S2LegacyValidQuery<MutableS2ShapeIndex> query;
  return !query.Validate(polygon_index, error);
}

class WKBWriter {
 public:
  std::string Point(const std::optional<S2Point>& point) {
    Encoder encoder;
    encoder.Ensure(21);
    WriteHeader(encoder, 1);
    if (point.has_value()) {
      const S2LatLng lng_lat(*point);
      WriteDouble(encoder, lng_lat.lng().degrees());
      WriteDouble(encoder, lng_lat.lat().degrees());
    } else {
      WriteDouble(encoder, NAN);
      WriteDouble(encoder, NAN);
    }
    return std::string(encoder.base(), encoder.length());
  }

  std::string Polygon(const S2Polygon& polygon) {
    std::vector<int> shells;
    for (int i = 0; i < polygon.num_loops(); i++) {
      if ((polygon.loop(i)->depth() % 2) == 0) {
        shells.push_back(i);
      }
    }

    Encoder encoder;
    if (shells.size() <= 1) {
      WritePolygon(encoder, polygon, shells.empty() ? -1 : shells[0]);
    } else {
      WriteHeader(encoder, 6);
      encoder.Ensure(sizeof(uint32_t));
      encoder.put32(static_cast<uint32_t>(shells.size()));
      for (const auto shell : shells) {
        WritePolygon(encoder, polygon, shell);
      }
    }
    return std::string(encoder.base(), encoder.length());
  }

 private:
  static void WriteHeader(Encoder& encoder, uint32_t geometry_type) {
    encoder.Ensure(sizeof(uint8_t) + sizeof(uint32_t));
    encoder.put8(1);
    encoder.put32(geometry_type);
  }

  static void WriteDouble(Encoder& encoder, double value) {
    encoder.Ensure(sizeof(uint64_t));
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    encoder.put64(bits);
  }

  static void WriteLoop(Encoder& encoder, const S2Loop& loop) {
    encoder.Ensure(sizeof(uint32_t));
    encoder.put32(static_cast<uint32_t>(loop.num_vertices() + 1));
    for (int i = 0; i <= loop.num_vertices(); i++) {
      const S2LatLng lng_lat(loop.vertex(i % loop.num_vertices()));
      WriteDouble(encoder, lng_lat.lng().degrees());
      WriteDouble(encoder, lng_lat.lat().degrees());
    }
  }

  static void WritePolygon(Encoder& encoder, const S2Polygon& polygon, int shell) {
    WriteHeader(encoder, 3);
    if (shell < 0) {
      encoder.Ensure(sizeof(uint32_t));
      encoder.put32(0);
      return;
    }

    uint32_t num_rings = 1;
    const auto shell_depth = polygon.loop(shell)->depth();
    for (int i = shell + 1;
         i < polygon.num_loops() && polygon.loop(i)->depth() > shell_depth; i++) {
      if (polygon.loop(i)->depth() == shell_depth + 1) {
        num_rings++;
      }
    }
    encoder.Ensure(sizeof(uint32_t));
    encoder.put32(num_rings);
    WriteLoop(encoder, *polygon.loop(shell));
    for (int i = shell + 1;
         i < polygon.num_loops() && polygon.loop(i)->depth() > shell_depth; i++) {
      if (polygon.loop(i)->depth() == shell_depth + 1) {
        WriteLoop(encoder, *polygon.loop(i));
      }
    }
  }
};

}  // namespace duckdb_s2
}  // namespace duckdb
