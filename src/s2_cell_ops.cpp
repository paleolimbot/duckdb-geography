
#include <cstdint>
#include <cstring>

#include "duckdb/main/database.hpp"

#include "s2/s2cell.h"
#include "s2/s2cell_union.h"
#include "s2geography/op/cell.h"
#include "s2geography/op/point.h"

#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

#include "function_builder.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

// Helper functions for endian-aware loading since s2geometry v0.12+ removed template
// Load<T>
inline double LoadDoubleLE(const void* p) {
  double result;
  std::memcpy(&result, p, sizeof(double));
  return result;
}

inline double LoadDoubleBE(const void* p) {
  uint64_t val;
  std::memcpy(&val, p, sizeof(uint64_t));
  // Byte swap for big endian
  val = ((val & 0x00000000000000FFULL) << 56) | ((val & 0x000000000000FF00ULL) << 40) |
        ((val & 0x0000000000FF0000ULL) << 24) | ((val & 0x00000000FF000000ULL) << 8) |
        ((val & 0x000000FF00000000ULL) >> 8) | ((val & 0x0000FF0000000000ULL) >> 24) |
        ((val & 0x00FF000000000000ULL) >> 40) | ((val & 0xFF00000000000000ULL) >> 56);
  double result;
  std::memcpy(&result, &val, sizeof(double));
  return result;
}

inline uint32_t LoadUInt32BE(const void* p) {
  uint32_t val;
  std::memcpy(&val, p, sizeof(uint32_t));
  // Byte swap for big endian
  return ((val & 0x000000FFU) << 24) | ((val & 0x0000FF00U) << 8) |
         ((val & 0x00FF0000U) >> 8) | ((val & 0xFF000000U) >> 24);
}

struct S2CellCenterFromGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, uint64_t>(
        source, result, count, [&](string_t geog_str) -> uint64_t {
          decoder.DecodeTag(geog_str);

          // Empties are always translated as invalid regardless of type
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return S2CellId::Sentinel().id();
          }

          // If we already have a snapped cell center encoding, the last 8 inlined
          // bytes are the little endian cell id as a uint64_t
          if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER &&
              decoder.tag.covering_size == 1) {
            uint64_t cell_id = LittleEndian::Load64(geog_str.GetData() + 4);
            return cell_id;
          }

          // Otherwise, we just need to load the geography
          std::unique_ptr<s2geography::Geography> geog = decoder.Decode(geog_str);

          // Use the Shape interface, which should work for PointGeography
          // and EncodedShapeIndex geography. A single shape with a single
          // edge always works here.
          if (geog->num_shapes() != 1) {
            return S2CellId::Sentinel().id();
          }

          std::unique_ptr<S2Shape> shape = geog->Shape(0);
          if (shape->num_edges() != 1 || shape->dimension() != 0) {
            throw InvalidInputException(
                std::string("Can't convert geography that is not empty nor a single "
                            "point to S2_CELL_CENTER"));
          }

          S2CellId cell(shape->edge(0).v0);
          return cell.id();
        });
  }
};

struct S2CellUnionFromS2Cell {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    ListVector::Reserve(result, count);
    uint64_t offset = 0;

    UnaryExecutor::Execute<uint64_t, list_entry_t>(
        source, result, count, [&](uint64_t cell_id) {
          S2CellId cell(cell_id);
          if (!cell.is_valid()) {
            return list_entry_t{0, 0};
          } else {
            ListVector::PushBack(result, Value::UBIGINT(cell_id));
            return list_entry_t{offset++, 1};
          }
        });
  }
};

// Normalize storage on the cast in to the type
struct S2CellUnionFromStorage {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    ListVector::Reserve(result, count);
    vector<S2CellId> cell_ids;
    // Not sure if this is the appropriate way to handle list child data
    // in the presence of a possible dictionary vector as a source
    auto child_ids = reinterpret_cast<uint64_t*>(ListVector::GetEntry(source).GetData());

    uint64_t offset = 0;

    UnaryExecutor::Execute<list_entry_t, list_entry_t>(
        source, result, count, [&](list_entry_t item) {
          cell_ids.resize(static_cast<size_t>(item.length));
          for (uint64_t i = 0; i < item.length; i++) {
            cell_ids[i] = S2CellId(child_ids[item.offset + i]);
            if (!cell_ids[i].is_valid()) {
              throw InvalidInputException(
                  std::string("Cell not valid <" + cell_ids[i].ToString() + ">"));
            }
          }

          S2CellUnion::Normalize(&cell_ids);
          for (const auto cell_id : cell_ids) {
            ListVector::PushBack(result, Value::UBIGINT(cell_id.id()));
          }

          list_entry_t out{offset, cell_ids.size()};
          offset += out.length;
          return out;
        });
  }
};

struct S2CellUnionToGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyEncoder encoder;
    vector<S2CellId> cell_ids;
    // Not sure if this is the appropriate way to handle list child data
    // in the presence of a possible dictionary vector as a source
    auto child_ids = reinterpret_cast<uint64_t*>(ListVector::GetEntry(source).GetData());

    UnaryExecutor::Execute<list_entry_t, string_t>(
        source, result, count, [&](list_entry_t item) {
          cell_ids.resize(static_cast<size_t>(item.length));
          for (uint64_t i = 0; i < item.length; i++) {
            cell_ids[i] = S2CellId(child_ids[item.offset + i]);
          }

          // If this step turns out to be a bottleneck, we can investigate
          // using S2CellUnion::FromNormalized() and requiring the caller to
          // explicitly normalize first.
          auto cells = S2CellUnion(std::move(cell_ids));
          auto poly = make_uniq<S2Polygon>();
          poly->InitToCellUnionBorder(cells);
          s2geography::PolygonGeography geog(std::move(poly));
          cell_ids = cells.Release();

          // Would be nice if we could set the covering here since we already
          // know exactly what it is!
          return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
        });
  }
};

// Experimental version of a WKB parser that only handles points (or multipoints
// with a single point). Also, includes an implementation of the s2ified
// GEOSHilbertCode_r() (which helpfully does not require a previously calculated extent).
struct S2CellCenterFromWKB {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cellfromwkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkb", LogicalType::BLOB);
            variant.SetReturnType(Types::S2_CELL_CENTER());
            variant.SetFunction(ExecutePointFn);
          });

          func.SetDescription(R"(
Convert a WKB point directly to S2_CELL_CENTER.

This is the same as `s2_geogfromwkb()::S2_CELL_CENTER` but does the parsing
directly to maximize performance. Cell centers are a highly efficient type
for storing point data where a precision loss of up to ~2cm is acceptable;
this function exists to ensure getting data into this format is as easy as
possible.

This function assumes the input WKB contains longitude/latitude coordinates
and will error for any input that is not a POINT or MULTIPOINT with exactly
one point.
)");
          func.SetExample(R"(
SELECT name, s2_cellfromwkb(s2_aswkb(geog)) as cell
FROM s2_data_cities()
LIMIT 5;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_arbitrarycellfromwkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkb", LogicalType::BLOB);
            variant.SetReturnType(Types::S2_CELL_CENTER());
            variant.SetFunction(ExecuteArbitraryFn);
          });

          func.SetDescription(R"(
Get an arbitrary S2_CELL_CENTER on or near the input.

This function parses the minimum required WKB input to obtain the first
longitude/latitude pair it sees and finds the closest S2_CELL_CENTER. This
is useful for sorting or partitioning of lon/lat input when there is no need
to create a GEOGRAPHY.

Note that longitude/latitude is assumed in the input.
)");
          func.SetExample(R"(
SELECT name, s2_arbitrarycellfromwkb(s2_aswkb(geog)) AS cell
FROM s2_data_cities()
LIMIT 5;
----
-- Use to partition arbitrary lon/lat input
COPY (
  SELECT
    geog.s2_aswkb().s2_arbitrarycellfromwkb().s2_cell_parent(2).s2_cell_token() AS partition_cell,
    name,
    geog.s2_aswkb()
  FROM s2_data_cities()
) TO 'cities' WITH (FORMAT PARQUET, PARTITION_BY partition_cell);

SELECT * FROM glob('cities/**') LIMIT 5;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }

  static inline void ExecutePointFn(DataChunk& args, ExpressionState& state,
                                    Vector& result) {
    return ExecutePoint(args.data[0], result, args.size());
  }

  static inline void ExecuteArbitraryFn(DataChunk& args, ExpressionState& state,
                                        Vector& result) {
    return ExecuteArbitrary(args.data[0], result, args.size());
  }

  // Here the goal is to parse POINT (x x) or MULTIPOINT ((x x)) into a cell id
  // and error for anything else. EMPTY input goes to Sentinel().
  static inline void ExecutePoint(Vector& source, Vector& result, idx_t count) {
    UnaryExecutor::Execute<string_t, uint64_t>(source, result, count, [&](string_t wkb) {
      Decoder decoder(wkb.GetData(), wkb.GetSize());
      S2CellId cell_id = S2CellId::Sentinel();
      VisitGeometry(
          &decoder,
          [&cell_id](uint32_t geometry_type, S2LatLng pt) {
            // If this point didn't come from a point, we need to error
            if ((geometry_type % 1000) != 1) {
              throw InvalidInputException(
                  "Can't parse WKB with non-point input to S2_CELL_CENTER");
            }

            // If we've already seen a point, we also need to error
            if (cell_id != S2CellId::Sentinel()) {
              throw InvalidInputException(
                  "Can't parse WKB with more than one point to S2_CELL_CENTER");
            }

            cell_id = S2CellId(pt.ToPoint());
            return true;
          },
          [](void) { throw InvalidInputException("Invalid WKB"); });

      return cell_id.id();
    });
  }

  // Here the goal is just to get any arbitrary cell from the first lon/lat value we
  // find. This does have to assume that the WKB is lon/lat. Great for sorting!
  // Would be much improved if we could also reproject the first xy value we find
  // so that nobody has to parse WKB just to do a vague spatial sort.
  static inline void ExecuteArbitrary(Vector& source, Vector& result, idx_t count) {
    UnaryExecutor::Execute<string_t, uint64_t>(source, result, count, [&](string_t wkb) {
      Decoder decoder(wkb.GetData(), wkb.GetSize());
      S2CellId cell_id = S2CellId::Sentinel();
      VisitGeometry(
          &decoder,
          [&cell_id](uint32_t geometry_type, S2LatLng pt) {
            // We don't care about geometry type here either, but we do want
            // to stop after the first point has been reached.
            cell_id = S2CellId(pt.ToPoint());
            return false;
          },
          // We don't care about invalid input here
          [](void) {});

      return cell_id.id();
    });
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitGeometry(Decoder* decoder, LatLngCallback on_point,
                            ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint8_t)) {
      on_error();
      return false;
    }
    uint8_t le = decoder->get8();

    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t geometry_type;
    if (le) {
      geometry_type = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      geometry_type = LoadUInt32BE(decoder->skip(sizeof(uint32_t)));
    }

    if (geometry_type & ewkb_srid_bit) {
      if (decoder->avail() < sizeof(uint32_t)) {
        on_error();
        return false;
      }

      decoder->skip(sizeof(uint32_t));
    }

    geometry_type &= ~(ewkb_srid_bit | ewkb_zm_bits);
    switch (geometry_type % 1000) {
      case 1:
        return VisitPoint(decoder, le, geometry_type, on_point, on_error);
      case 2:
        return VisitSequence(decoder, le, geometry_type, on_point, on_error);
      case 3:
        return VisitPolygon(decoder, le, geometry_type, on_point, on_error);
      case 4:
      case 5:
      case 6:
      case 7:
        return VisitCollection(decoder, le, on_point, on_error);
      default:
        on_error();
        return false;
    }
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitCollection(Decoder* decoder, bool le, LatLngCallback on_point,
                              ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t n;
    if (le) {
      n = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      n = LoadUInt32BE(decoder->skip(sizeof(uint32_t)));
    }

    for (uint32_t i = 0; i < n; i++) {
      bool keep_going = VisitGeometry(decoder, on_point, on_error);
      if (!keep_going) {
        return false;
      }
    }

    return true;
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitPolygon(Decoder* decoder, bool le, uint32_t geometry_type,
                           LatLngCallback on_point, ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t n;
    if (le) {
      n = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      n = LoadUInt32BE(decoder->skip(sizeof(uint32_t)));
    }

    for (uint32_t i = 0; i < n; i++) {
      bool keep_going = VisitSequence(decoder, le, geometry_type, on_point, on_error);
      if (!keep_going) {
        return false;
      }
    }

    return true;
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitSequence(Decoder* decoder, bool le, uint32_t geometry_type,
                            LatLngCallback on_point, ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t n;
    if (le) {
      n = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      n = LoadUInt32BE(decoder->skip(sizeof(uint32_t)));
    }

    for (uint32_t i = 0; i < n; i++) {
      bool keep_going = VisitPoint(decoder, le, geometry_type, on_point, on_error);
      if (!keep_going) {
        return false;
      }
    }

    return true;
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitPoint(Decoder* decoder, bool le, uint32_t geometry_type,
                         LatLngCallback on_point, ErrorCallback on_error) {
    if (decoder->avail() < (2 * sizeof(double))) {
      on_error();
      return false;
    }

    double lnglat[2];
    if (le) {
      lnglat[0] = LoadDoubleLE(decoder->skip(sizeof(double)));
      lnglat[1] = LoadDoubleLE(decoder->skip(sizeof(double)));
    } else {
      lnglat[0] = LoadDoubleBE(decoder->skip(sizeof(double)));
      lnglat[1] = LoadDoubleBE(decoder->skip(sizeof(double)));
    }

    if (std::isnan(lnglat[0]) || std::isnan(lnglat[1])) {
      return true;
    }

    auto latlng = S2LatLng::FromDegrees(lnglat[1], lnglat[0]);
    return on_point(geometry_type, latlng);
  }

  static constexpr uint32_t ewkb_srid_bit = 0x20000000;
  static constexpr uint32_t ewkb_zm_bits = 0x40000000 | 0x80000000;
};

struct S2CellCenterFromLonLat {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cellfromlonlat", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("lon", LogicalType::DOUBLE);
            variant.AddParameter("lat", LogicalType::DOUBLE);
            variant.SetReturnType(Types::S2_CELL_CENTER());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Convert a lon/lat pair to S2_CELL_CENTER.

Cell centers are a highly efficient type for storing point data where a
precision loss of up to ~2cm is acceptable.

See [`s2_x()`](#s2_x) and [`s2_y()`](#s2_y) for the reverse operation.
)");
          func.SetExample(R"(
SELECT s2_cellfromlonlat(-64, 45);
----
SELECT name, s2_cellfromlonlat(s2_x(geog), s2_y(geog)) as cell
FROM s2_data_cities()
LIMIT 5;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], args.data[1], result, args.size());
  }

  static inline void Execute(Vector& src_lon, Vector& src_lat, Vector& result,
                             idx_t count) {
    BinaryExecutor::Execute<double, double, uint64_t>(
        src_lon, src_lat, result, count, [&](double lon, double lat) {
          if (std::isnan(lon) && std::isnan(lat)) {
            return static_cast<uint64_t>(S2CellId::Sentinel().id());
          }

          auto latlng = S2LatLng::FromDegrees(lat, lon);
          S2CellId cell_id(latlng.ToPoint());
          return static_cast<uint64_t>(cell_id.id());
        });
  }
};

struct S2CellCenterToGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    // Most cells will get serialized as a tag + cell_id
    Encoder non_empty;
    s2geography::EncodeTag tag;
    tag.kind = s2geography::GeographyKind::CELL_CENTER;
    tag.covering_size = 1;
    tag.Encode(&non_empty);
    non_empty.Ensure(sizeof(uint64_t));
    non_empty.put64(0);
    string_t non_empty_str{non_empty.base(), static_cast<uint32_t>(non_empty.length())};
    char* non_empty_cell_id = non_empty_str.GetPrefixWriteable() + 4;

    // Invalid cells will get serialized as an empty point
    Encoder empty;
    tag.kind = s2geography::GeographyKind::POINT;
    tag.covering_size = 0;
    tag.flags |= s2geography::EncodeTag::kFlagEmpty;
    tag.Encode(&empty);
    string_t empty_str{empty.base(), static_cast<uint32_t>(empty.length())};

    UnaryExecutor::Execute<uint64_t, string_t>(source, result, count, [&](uint64_t arg0) {
      S2CellId cell(arg0);
      if (cell.is_valid()) {
        std::memcpy(non_empty_cell_id, &arg0, sizeof(arg0));
        return non_empty_str;
      } else {
        return empty_str;
      }
    });
  }
};

struct S2CellToGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyEncoder encoder;

    UnaryExecutor::Execute<uint64_t, string_t>(source, result, count, [&](uint64_t arg0) {
      S2CellId cell(arg0);
      if (!cell.is_valid()) {
        s2geography::PolygonGeography geog;
        return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
      }

      auto loop = make_uniq<S2Loop>(S2Cell(cell));
      auto poly = make_uniq<S2Polygon>(std::move(loop));
      s2geography::PolygonGeography geog(std::move(poly));
      return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
    });
  }
};

struct S2CellVertex {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_vertex", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell_id", Types::S2_CELL());
            variant.AddParameter("vertex_id", LogicalType::INTEGER);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(Execute);
          });

          func.SetDescription(R"(
Extract a vertex (corner) of an S2 cell.

An S2_CELL is represented by an unsigned 64-bit integer but logically
represents a polygon with four vertices. This function extracts one of them
according to `vertex_id` (an integer from 0-3).

It is usually more convenient to cast an S2_CELL to GEOGRAPHY or pass an
S2_CELL directly to a function that accepts a GEOGRAPHY an use the implicit
conversion.
)");
          func.SetExample(R"(
SELECT s2_cell_vertex('5/'::S2_CELL, id) as vertex,
FROM (VALUES (0), (1), (2), (3)) vertices(id);
----

-- Usually easier to cast to GEOGRAPHY
SELECT '5/'::S2_CELL::GEOGRAPHY as geog;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    using s2geography::op::point::Point;
    s2geography::op::cell::CellVertex op;
    GeographyEncoder encoder;

    BinaryExecutor::Execute<uint64_t, int32_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](uint64_t cell_id, int32_t vertex_id) {
          Point pt = op.ExecuteScalar(cell_id, static_cast<int8_t>(vertex_id));
          s2geography::PointGeography geog({pt[0], pt[1], pt[2]});
          return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
        });
  }
};

template <typename Op>
struct S2CellToString {
  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], result, args.size());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    Op op;
    UnaryExecutor::Execute<uint64_t, string_t>(source, result, count, [&](uint64_t arg0) {
      std::string_view str = op.ExecuteScalar(arg0);
      return StringVector::AddString(
          result, string_t{str.data(), static_cast<uint32_t>(str.size())});
    });
  }
};

struct S2CellToToken {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_token", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell", Types::S2_CELL());
            variant.SetReturnType(LogicalType::VARCHAR);
            variant.SetFunction(
                S2CellToString<s2geography::op::cell::ToToken>::ExecuteFn);
          });

          func.SetDescription(R"(
Serialize an S2_CELL as a compact hexadecimal token.

To serialize to a more user-friendly (but longer) string, cast an `S2_CELL`
to `VARCHAR`.
)");
          func.SetExample(R"(
SELECT s2_cell_token(s2_cellfromlonlat(-64, 45));
----
SELECT s2_cell_token('5/3301'::S2_CELL);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }
};

template <typename Op>
struct S2CellFromString {
  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], result, args.size());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    Op op;
    UnaryExecutor::Execute<string_t, uint64_t>(source, result, count, [&](string_t arg0) {
      std::string_view item_view{arg0.GetData(), static_cast<size_t>(arg0.GetSize())};
      return static_cast<uint64_t>(op.ExecuteScalar(item_view));
    });
  }
};

struct S2CellFromToken {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_from_token", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("text", LogicalType::VARCHAR);
            variant.SetReturnType(Types::S2_CELL());
            variant.SetFunction(
                S2CellFromString<s2geography::op::cell::FromToken>::ExecuteFn);
          });

          func.SetDescription(R"(
Parse a hexadecimal token as an S2_CELL.

Note that invalid strings are given an invalid cell value of 0 but do not error.
To parse the more user-friendly debug string format, cast from `VARCHAR` to
`S2_CELL`.
)");
          func.SetExample(R"(
SELECT s2_cell_from_token('4b59a0cd83b5de49');
----
-- Invalid strings don't error but do parse into an invalid cell id
SELECT s2_cell_from_token('foofy');
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }
};

struct S2CellLevel {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_level", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell", Types::S2_CELL());
            variant.SetReturnType(LogicalType::TINYINT);
            variant.SetFunction(Execute);
          });

          func.SetDescription(R"(
Extract the level (0-30, inclusive) from an S2_CELL.
)");
          func.SetExample(R"(
SELECT s2_cell_level('5/33120'::S2_CELL);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    s2geography::op::cell::Level op;
    UnaryExecutor::Execute<uint64_t, int8_t>(
        args.data[0], result, args.size(),
        [&](uint64_t arg0) { return op.ExecuteScalar(arg0); });
  }
};

template <typename Op>
struct S2BinaryCellPredicate {
  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    BinaryExecutor::Execute<uint64_t, uint64_t, bool>(
        args.data[0], args.data[1], result, args.size(),
        [&](uint64_t arg0, uint64_t arg1) { return op.ExecuteScalar(arg0, arg1); });
  }
};

struct S2CellIntersects {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_intersects", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell1", Types::S2_CELL());
            variant.AddParameter("cell2", Types::S2_CELL());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(
                S2BinaryCellPredicate<s2geography::op::cell::MayIntersect>::Execute);
          });

          func.SetDescription(R"(
Return true if `cell1` contains `cell2` or `cell2` contains `cell1`.

See [`s2_cell_range_min()`](#s2_cell_range_min) and [`s2_cell_range_max()`](#s2_cell_range_max)
for how to calculate this in a way that DuckDB can use to accelerate a join.

Note that this will return false for neighboring cells. Use [`s2_intersects()`](#s2_intersects)
if you need this type of intersection check.
)");
          func.SetExample(R"(
SELECT s2_cell_intersects('5/3'::S2_CELL, '5/30'::S2_CELL) AS result;
----
SELECT s2_cell_intersects('5/30'::S2_CELL, '5/3'::S2_CELL) AS result;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }
};

struct S2CellContains {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_contains", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell1", Types::S2_CELL());
            variant.AddParameter("cell2", Types::S2_CELL());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(
                S2BinaryCellPredicate<s2geography::op::cell::Contains>::Execute);
          });

          func.SetDescription(R"(
Return true if `cell1` contains `cell2`.

See [`s2_cell_range_min()`](#s2_cell_range_min) and [`s2_cell_range_max()`](#s2_cell_range_max)
for how to calculate this in a way that DuckDB can use to accelerate a join.
)");
          func.SetExample(R"(
SELECT s2_cell_contains('5/3'::S2_CELL, '5/30'::S2_CELL) AS result;
----
SELECT s2_cell_contains('5/30'::S2_CELL, '5/3'::S2_CELL) AS result;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }
};

template <typename Op>
struct S2CellToCell {
  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    BinaryExecutor::Execute<uint64_t, int32_t, uint64_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](uint64_t arg0, int32_t arg1) {
          return static_cast<uint64_t>(op.ExecuteScalar(arg0, static_cast<int8_t>(arg1)));
        });
  }
};

struct S2CellChild {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_child", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell", Types::S2_CELL());
            variant.AddParameter("index", LogicalType::INTEGER);
            variant.SetReturnType(Types::S2_CELL());
            variant.SetFunction(S2CellToCell<s2geography::op::cell::Child>::Execute);
          });

          func.SetDescription(R"(
Compute a child S2_CELL.

Each S2_CELL that is not a leaf cell (level 30) has exactly four children
(index 0-3 inclusive). Values for `index` outside this range will result in
an invalid returned cell.
)");
          func.SetExample(R"(
SELECT s2_cell_child('5/00000'::S2_CELL, ind) as cell
FROM (VALUES (0), (1), (2), (3), (4)) indices(ind);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }
};

struct S2CellParent {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_parent", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell", Types::S2_CELL());
            variant.AddParameter("level", LogicalType::INTEGER);
            variant.SetReturnType(Types::S2_CELL());
            variant.SetFunction(S2CellToCell<s2geography::op::cell::Parent>::Execute);
          });

          func.SetDescription(R"(
Compute a parent S2_CELL.

Note that level is clamped to the valid range 0-30. A negative value will
be subtracted from the current level (e.g., use `-1` for the immediate parent).
)");
          func.SetExample(R"(
SELECT s2_cell_parent(s2_cellfromlonlat(-64, 45), level) as cell
FROM (VALUES (0), (1), (2), (3), (4), (5), (-1), (-2)) levels(level);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }
};

struct S2CellEdgeNeighbor {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_edge_neighbor", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell", Types::S2_CELL());
            variant.AddParameter("index", LogicalType::INTEGER);
            variant.SetReturnType(Types::S2_CELL());
            variant.SetFunction(
                S2CellToCell<s2geography::op::cell::EdgeNeighbor>::Execute);
          });

          func.SetDescription(R"(
Compute a neighbor S2_CELL.

Every S2_CELL has a neighbor at the top, left, right, and bottom,
which can be selected from index values 0-3 (inclusive). Values of
`index` outside this range will result in an invalid returned cell value.
)");
          func.SetExample(R"(
SELECT s2_cell_edge_neighbor('5/00000'::S2_CELL, ind) as cell
FROM (VALUES (0), (1), (2), (3), (4)) indices(ind);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }
};

struct S2CellBounds {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_range_min", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell", Types::S2_CELL());
            variant.SetReturnType(Types::S2_CELL());
            variant.SetFunction(ExecuteRangeMin);
          });

          func.SetDescription(R"(
Compute the minimum leaf cell value contained within an S2_CELL.
)");
          func.SetExample(R"(
SELECT
  s2_cell_range_min('5/00000'::S2_CELL) AS cell_min,
  s2_cell_range_max('5/00000'::S2_CELL) AS cell_max;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_cell_range_max", [&](ScalarFunctionBuilder& func) {
          func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell", Types::S2_CELL());
            variant.SetReturnType(Types::S2_CELL());
            variant.SetFunction(ExecuteRangeMax);
          });

          func.SetDescription(R"(
Compute the maximum leaf cell value contained within an S2_CELL.
)");
          func.SetExample(R"(
SELECT
  s2_cell_range_min('5/00000'::S2_CELL) AS cell_min,
  s2_cell_range_max('5/00000'::S2_CELL) AS cell_max;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }

  static inline void ExecuteRangeMin(DataChunk& args, ExpressionState& state,
                                     Vector& result) {
    UnaryExecutor::Execute<uint64_t, uint64_t>(args.data[0], result, args.size(),
                                               [&](uint64_t cell_id) {
                                                 S2CellId cell(cell_id);
                                                 if (!cell.is_valid()) {
                                                   return S2CellId::Sentinel().id();
                                                 } else {
                                                   return cell.range_min().id();
                                                 }
                                               });
  }

  static inline void ExecuteRangeMax(DataChunk& args, ExpressionState& state,
                                     Vector& result) {
    UnaryExecutor::Execute<uint64_t, uint64_t>(args.data[0], result, args.size(),
                                               [&](uint64_t cell_id) {
                                                 S2CellId cell(cell_id);
                                                 if (!cell.is_valid()) {
                                                   return S2CellId::Sentinel().id();
                                                 } else {
                                                   return cell.range_max().id();
                                                 }
                                               });
  }
};

bool ExecuteNoopCast(Vector& source, Vector& result, idx_t count,
                     CastParameters& parameters) {
  result.Reinterpret(source);
  return true;
}

}  // namespace

void RegisterS2CellOps(ExtensionLoader& loader) {
  using namespace s2geography::op::cell;

  // Explicit casts to/from string handle the debug string (better for printing)
  // We use the same character representation for both cells and centers.
  loader.RegisterCastFunction(Types::S2_CELL(), LogicalType::VARCHAR,
                              BoundCastInfo(S2CellToString<ToDebugString>::ExecuteCast),
                              1);
  loader.RegisterCastFunction(
      LogicalType::VARCHAR, Types::S2_CELL(),
      BoundCastInfo(S2CellFromString<FromDebugString>::ExecuteCast), 1);
  loader.RegisterCastFunction(Types::S2_CELL_CENTER(), LogicalType::VARCHAR,
                              BoundCastInfo(S2CellToString<ToDebugString>::ExecuteCast),
                              1);
  loader.RegisterCastFunction(
      LogicalType::VARCHAR, Types::S2_CELL_CENTER(),
      BoundCastInfo(S2CellFromString<FromDebugString>::ExecuteCast), 1);

  // s2_cell_center to geography can be implicit (never fails for valid input)
  loader.RegisterCastFunction(Types::S2_CELL_CENTER(), Types::GEOGRAPHY(),
                              BoundCastInfo(S2CellCenterToGeography::ExecuteCast), 0);

  // geography to s2_cell_center must be explicit (can move a point up to 1 cm,
  // fails for input that is not a single point)
  loader.RegisterCastFunction(Types::GEOGRAPHY(), Types::S2_CELL_CENTER(),
                              BoundCastInfo(S2CellCenterFromGeography::ExecuteCast), 1);

  // s2_cell to geography can be implicit (never fails for valid input)
  loader.RegisterCastFunction(Types::S2_CELL(), Types::GEOGRAPHY(),
                              BoundCastInfo(S2CellToGeography::ExecuteCast), 0);

  // s2_cell_union to geography can be implicit
  loader.RegisterCastFunction(Types::S2_CELL_UNION(), Types::GEOGRAPHY(),
                              BoundCastInfo(S2CellUnionToGeography::ExecuteCast), 0);

  // s2_cell to s2_cell_union can be implicit
  loader.RegisterCastFunction(Types::S2_CELL(), Types::S2_CELL_UNION(),
                              BoundCastInfo(S2CellUnionFromS2Cell::ExecuteCast), 0);

  // s2_cell union from storage is explicit
  loader.RegisterCastFunction(LogicalType::LIST(Types::S2_CELL()), Types::S2_CELL_UNION(),
                              BoundCastInfo(S2CellUnionFromStorage::ExecuteCast), 1);
  loader.RegisterCastFunction(LogicalType::LIST(LogicalType::UBIGINT),
                              Types::S2_CELL_UNION(),
                              BoundCastInfo(S2CellUnionFromStorage::ExecuteCast), 1);
  loader.RegisterCastFunction(LogicalType::LIST(LogicalType::BIGINT),
                              Types::S2_CELL_UNION(),
                              BoundCastInfo(S2CellUnionFromStorage::ExecuteCast), 1);

  // Explicit casts: s2_cell to/from s2_cell_center
  loader.RegisterCastFunction(Types::S2_CELL_CENTER(), Types::S2_CELL(),
                              BoundCastInfo(ExecuteNoopCast), 1);
  loader.RegisterCastFunction(Types::S2_CELL(), Types::S2_CELL_CENTER(),
                              BoundCastInfo(ExecuteNoopCast), 1);

  S2CellCenterFromWKB::Register(loader);
  S2CellCenterFromLonLat::Register(loader);
  S2CellToToken::Register(loader);
  S2CellFromToken::Register(loader);

  S2CellLevel::Register(loader);

  S2CellVertex::Register(loader);

  S2CellContains::Register(loader);
  S2CellIntersects::Register(loader);

  S2CellChild::Register(loader);
  S2CellParent::Register(loader);
  S2CellEdgeNeighbor::Register(loader);

  S2CellBounds::Register(loader);
}

}  // namespace duckdb_s2
}  // namespace duckdb
