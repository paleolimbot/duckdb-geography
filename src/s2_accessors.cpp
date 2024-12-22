
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "function_builder.hpp"

#include "s2/s2earth.h"
#include "s2geography/accessors.h"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2IsEmpty {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_isempty", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription("Returns true if the geography is empty.");
          func.SetExample("SELECT s2_isempty('POINT(0 0)') AS is_empty;");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, bool>(source, result, count, [&](string_t geog_str) {
      decoder.DecodeTag(geog_str);
      return decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty;
    });
  }
};

struct S2IsValid {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_is_valid", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Returns true if the geography is valid.

The most common reasons for invalid geographies are repeated points,
an inadequate number of points, and/or crossing edges.
)");
          func.SetExample(R"(
SELECT s2_is_valid(s2_geogfromtext_novalidate('LINESTRING (0 0, 1 1)')) AS valid;
----
SELECT s2_is_valid(s2_geogfromtext_novalidate('LINESTRING (0 0, 0 0, 1 1)')) AS valid;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;
    S2Error error;

    UnaryExecutor::Execute<string_t, bool>(source, result, count, [&](string_t geog_str) {
      decoder.DecodeTag(geog_str);
      if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
        return true;
      } else if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER) {
        return true;
      }

      auto geog = decoder.Decode(geog_str);
      return !s2geography::s2_find_validation_error(*geog, &error);
    });
  }
};

struct S2IsValidReason {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_is_valid_reason", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::VARCHAR);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Returns the error string for invalid geographies or the empty string ("") otherwise.
)");
          func.SetExample(R"(
SELECT s2_is_valid_reason(s2_geogfromtext_novalidate('LINESTRING (0 0, 1 1)')) AS valid;
----
SELECT s2_is_valid_reason(s2_geogfromtext_novalidate('LINESTRING (0 0, 0 0, 1 1)')) AS valid;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;
    S2Error error;

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return string_t{""};
          } else if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER) {
            return string_t{""};
          }

          auto geog = decoder.Decode(geog_str);
          error.Clear();
          if (!s2geography::s2_find_validation_error(*geog, &error)) {
            return string_t{""};
          } else {
            return StringVector::AddString(result, error.text());
          }
        });
  }
};

struct S2Area {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(instance, "s2_area", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog", Types::GEOGRAPHY());
        variant.SetReturnType(LogicalType::DOUBLE);
        variant.SetFunction(ExecuteFn);
      });

      func.SetDescription(R"(
Calculate the area of the geography in square meters.

The returned area is in square meters as approximated as the area of the polygon
on a perfect sphere.

For non-polygon geographies, `s2_area()` returns `0.0`.
)");
      func.SetExample(R"(
SELECT s2_area(s2_data_country('Fiji')) AS area;
----
SELECT s2_area('POINT (0 0)'::GEOGRAPHY) AS area;
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "accessors");
    });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return 0.0;
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER:
            case s2geography::GeographyKind::POINT:
            case s2geography::GeographyKind::POLYLINE:
              return 0.0;
            default: {
              auto geog = decoder.Decode(geog_str);
              return s2geography::s2_area(*geog) * S2Earth::RadiusMeters() *
                     S2Earth::RadiusMeters();
            }
          }
        });
  }
};

struct S2Perimieter {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_perimeter", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::DOUBLE);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Calculate the perimeter of the geography in meters.

The returned length is in meters as approximated as the perimeter of the polygon
on a perfect sphere.

For non-polygon geographies, `s2_perimeter()` returns `0.0`. For a  polygon with
more than one ring, this function returns the sum of the perimeter of all
rings.
)");
          func.SetExample(R"(
SELECT s2_perimeter(s2_data_country('Fiji')) AS perimeter;
----
SELECT s2_perimeter('POINT (0 0)'::GEOGRAPHY) AS perimeter;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return 0.0;
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER:
            case s2geography::GeographyKind::POINT:
            case s2geography::GeographyKind::POLYLINE:
              return 0.0;
            default: {
              auto geog = decoder.Decode(geog_str);
              return s2geography::s2_perimeter(*geog) * S2Earth::RadiusMeters();
            }
          }
        });
  }
};

struct S2Length {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_length", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::DOUBLE);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Calculate the length of the geography in meters.

For non-linestring or multilinestring geographies, `s2_length()` returns `0.0`.
)");
          func.SetExample(R"(
SELECT s2_length('POINT (0 0)'::GEOGRAPHY) AS length;
----
SELECT s2_length('LINESTRING (0 0, -64 45)'::GEOGRAPHY) AS length;
----
SELECT s2_length(s2_data_country('Canada')) AS length;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return 0.0;
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER:
            case s2geography::GeographyKind::POINT:
            case s2geography::GeographyKind::POLYGON:
              return 0.0;
            default: {
              auto geog = decoder.Decode(geog_str);
              return s2geography::s2_length(*geog) * S2Earth::RadiusMeters();
            }
          }
        });
  }
};

struct S2XY {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(instance, "s2_x", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog", Types::GEOGRAPHY());
        variant.SetReturnType(LogicalType::DOUBLE);
        variant.SetFunction(ExecuteFnX);
      });

      func.SetDescription(R"(
Extract the longitude of a point geography.

For geographies that are not a single point, `NaN` is returned.
)");

      func.SetExample(R"(
SELECT s2_x('POINT (-64 45)'::GEOGRAPHY);
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "accessors");
    });

    FunctionBuilder::RegisterScalar(instance, "s2_y", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog", Types::GEOGRAPHY());
        variant.SetReturnType(LogicalType::DOUBLE);
        variant.SetFunction(ExecuteFnY);
      });

      func.SetDescription(R"(
Extract the latitude of a point geography.

For geographies that are not a single point, `NaN` is returned.
)");

      func.SetExample(R"(
SELECT s2_y('POINT (-64 45)'::GEOGRAPHY);
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "accessors");
    });
  }

  static inline void ExecuteFnX(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(
        args.data[0], result, args.size(), [](S2LatLng ll) { return ll.lng().degrees(); },
        [](const s2geography::Geography& geog) { return s2_x(geog); });
  }

  static inline void ExecuteFnY(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(
        args.data[0], result, args.size(), [](S2LatLng ll) { return ll.lat().degrees(); },
        [](const s2geography::Geography& geog) { return s2_y(geog); });
  }

  template <typename HandleLatLng, typename HandleGeog>
  static void Execute(Vector& source, Vector& result, idx_t count,
                      HandleLatLng&& handle_latlng, HandleGeog&& handle_geog) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return static_cast<double>(NAN);
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER: {
              decoder.DecodeTagAndCovering(geog_str);
              S2Point center = decoder.covering[0].ToPoint();
              return handle_latlng(S2LatLng(center));
            }

            default: {
              auto geog = decoder.Decode(geog_str);
              return handle_geog(*geog);
            }
          }
        });
  }
};

}  // namespace

void RegisterS2GeographyAccessors(DatabaseInstance& instance) {
  S2IsEmpty::Register(instance);
  S2IsValid::Register(instance);
  S2IsValidReason::Register(instance);
  S2Area::Register(instance);
  S2Perimieter::Register(instance);
  S2Length::Register(instance);
  S2XY::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
