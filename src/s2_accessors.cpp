
#include "duckdb/main/database.hpp"

#include "arrow_scalar_udf.hpp"
#include "function_builder.hpp"
#include "s2_kernels.hpp"
#include "s2_wkb.hpp"

#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2IsEmpty {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_isempty", [](ScalarFunctionBuilder& func) {
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
    WKBGeography reader;

    UnaryExecutor::Execute<string_t, bool>(source, result, count, [&](string_t geog_str) {
      return reader.Read(geog_str).is_empty();
    });
  }
};

struct S2IsValid {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_is_valid", [](ScalarFunctionBuilder& func) {
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
    WKBGeography reader;
    S2Error error;

    UnaryExecutor::Execute<string_t, bool>(source, result, count, [&](string_t geog_str) {
      const auto& geography = reader.Read(geog_str);
      if (geography.is_empty()) {
        return true;
      }
      return !FindValidationError(geography, &error);
    });
  }
};

struct S2IsValidReason {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_is_valid_reason", [](ScalarFunctionBuilder& func) {
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
    WKBGeography reader;
    S2Error error;

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t geog_str) {
          const auto& geography = reader.Read(geog_str);
          if (geography.is_empty()) {
            return string_t{""};
          }

          error = S2Error();  // Reset error state
          if (!FindValidationError(geography, &error)) {
            return string_t{""};
          } else {
            auto msg = error.message();
            return StringVector::AddString(result, msg.data(), msg.size());
          }
        });
  }
};

struct S2Area {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(loader, "s2_area", [](ScalarFunctionBuilder& func) {
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

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::AreaKernel>(args, state, result);
  }
};

struct S2Perimieter {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_perimeter", [](ScalarFunctionBuilder& func) {
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

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::PerimeterKernel>(args, state, result);
  }
};

struct S2Length {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(loader, "s2_length", [](ScalarFunctionBuilder& func) {
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

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ArrowScalarFunction<s2geography::sedona_udf::LengthKernel>(args, state, result);
  }
};

struct S2XY {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(loader, "s2_x", [](ScalarFunctionBuilder& func) {
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

    FunctionBuilder::RegisterScalar(loader, "s2_y", [](ScalarFunctionBuilder& func) {
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
    Execute(args.data[0], result, args.size(),
            [](S2LatLng lng_lat) { return lng_lat.lng().degrees(); });
  }

  static inline void ExecuteFnY(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size(),
            [](S2LatLng lng_lat) { return lng_lat.lat().degrees(); });
  }

  template <typename HandleLatLng>
  static void Execute(Vector& source, Vector& result, idx_t count,
                      HandleLatLng&& handle_latlng) {
    WKBGeography reader;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          const auto& geography = reader.Read(geog_str);
          if (geography.points()->num_edges() != 1) {
            return static_cast<double>(NAN);
          }
          return handle_latlng(S2LatLng(geography.points()->edge(0).v0));
        });
  }
};

struct S2Dimension {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_dimension", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::INTEGER);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Calculate the highest dimension element present in the geography.

Points have a dimension of 0; linestrings have a dimension of 1; polygons have
a dimension of 2. For geography collections, this will return the highest dimension
value of any element in the collection (e.g., a collection containing a point and
a polygon will return 2). An empty geography collection returns a value of -1.
)");
          func.SetExample(R"(
SELECT s2_dimension('POINT (0 0)'::GEOGRAPHY);
----
SELECT s2_dimension('LINESTRING (0 0, 1 1)'::GEOGRAPHY);
----
SELECT s2_dimension(s2_data_country('Canada'));
----
SELECT s2_dimension('GEOMETRYCOLLECTION EMPTY');
----
SELECT s2_dimension('GEOMETRYCOLLECTION (POINT (0 1), LINESTRING (0 0, 1 1))'::GEOGRAPHY);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    WKBGeography reader;

    UnaryExecutor::Execute<string_t, int32_t>(
        source, result, count,
        [&](string_t geog_str) { return reader.Read(geog_str).max_dimension(); });
  }
};

struct S2NumPoints {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_num_points", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::INTEGER);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Extract the number of vertices in the geography.
)");
          func.SetExample(R"(
SELECT s2_num_points(s2_data_country('Fiji'));
----
SELECT s2_num_points(s2_data_country('Canada'));
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "accessors");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    WKBGeography reader;

    UnaryExecutor::Execute<string_t, int32_t>(
        source, result, count, [&](string_t geog_str) {
          const auto& geography = reader.Read(geog_str);
          return geography.points()->num_edges() + geography.lines()->num_edges() +
                 geography.lines()->num_chains() + geography.polygons()->num_edges();
        });
  }
};

}  // namespace

void RegisterS2GeographyAccessors(ExtensionLoader& loader) {
  S2IsEmpty::Register(loader);
  S2IsValid::Register(loader);
  S2IsValidReason::Register(loader);
  S2Area::Register(loader);
  S2Perimieter::Register(loader);
  S2Length::Register(loader);
  S2XY::Register(loader);
  S2Dimension::Register(loader);
  S2NumPoints::Register(loader);
}

}  // namespace duckdb_s2
}  // namespace duckdb
