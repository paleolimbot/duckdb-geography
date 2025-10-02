
#include "duckdb/main/database.hpp"

#include "s2/encoded_s2shape_index.h"
#include "s2/s2shape_index_region.h"
#include "s2/s2shapeutil_coding.h"
#include "s2geography/geography.h"

#include "s2_geography_serde.hpp"
#include "s2_types.hpp"
#include "s2geography/geoarrow.h"
#include "s2geography/wkb.h"
#include "s2geography/wkt-reader.h"
#include "s2geography/wkt-writer.h"

#include "function_builder.hpp"

namespace duckdb {

namespace duckdb_s2 {

struct S2GeogFromText {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_geogfromtext", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkt", LogicalType::VARCHAR);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Returns the geography from a WKT string.

This is an alias for the cast from VARCHAR to GEOGRAPHY. This
function assumes spherical edges.
)");
          func.SetExample(R"(
SELECT s2_geogfromtext('POINT (0 1)');
----
SELECT 'POINT (0 1)'::GEOGRAPHY;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_geogfromtext_novalidate", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkt", LogicalType::VARCHAR);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFnNovalidate);
          });

          func.SetDescription(R"(
Returns the geography from a WKT string skipping validation.

This is useful to determine which of some set of geometries is not valid and
why.
)");
          func.SetExample(R"(
SELECT s2_geogfromtext_novalidate('LINESTRING (0 0, 0 0, 1 1)');
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });

    loader.RegisterCastFunction(LogicalType::VARCHAR, Types::GEOGRAPHY(),
                                BoundCastInfo(ExecuteCast), 1);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void ExecuteFnNovalidate(DataChunk& args, ExpressionState& state,
                                         Vector& result) {
    s2geography::geoarrow::ImportOptions options;
    options.set_check(false);
    Execute(args.data[0], result, args.size(), options);
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count,
                             const s2geography::geoarrow::ImportOptions& options =
                                 s2geography::geoarrow::ImportOptions()) {
    GeographyEncoder encoder;
    s2geography::WKTReader reader(options);

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkt) {
      auto geog = reader.read_feature(wkt.GetData(), wkt.GetSize());
      return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
    });
  }
};

struct S2AsText {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(loader, "s2_astext", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog", Types::GEOGRAPHY());
        variant.SetReturnType(LogicalType::VARCHAR);
        variant.SetFunction(ExecuteFn);
      });

      func.SetDescription(R"(
Returns the well-known text (WKT) string of the geography.

Note that because the internal representation of the GEOGRAPHY type is either
an S2_CELL_CENTER or a unit vector, WKT typically does not roundtrip through a
GEOGRAPHY unless the output is rounded using `[s2_format()`][#s2_format].

The output contains spherical edges. If edges are large and the consumer does
not know that the edges are spherical, this may cause issues.

Calling this function has the same effect as casting to VARCHAR.
)");
      func.SetExample(R"(
SELECT s2_astext(s2_data_city('Vancouver'));
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "conversion");
    });

    FunctionBuilder::RegisterScalar(loader, "s2_format", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog", Types::GEOGRAPHY());
        variant.AddParameter("precision", LogicalType::TINYINT);
        variant.SetReturnType(LogicalType::VARCHAR);
        variant.SetFunction(ExecuteFnPrec);
      });

      func.SetDescription(
          R"(
Returns the WKT string of the geography with a given precision.

See [`s2_astext()`](#s2_text) for parameter-free lossless output. Like `s2_text()`,
this function exports spherical edges.
)");
      func.SetExample(R"(
SELECT s2_format(s2_data_city('Vancouver'), 1);
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "conversion");
    });

    loader.RegisterCastFunction(Types::GEOGRAPHY(), LogicalType::VARCHAR,
                                BoundCastInfo(ExecuteCast), 1);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void ExecuteFnPrec(DataChunk& args, ExpressionState& state,
                                   Vector& result) {
    Vector& precision = args.data[1];
    if (precision.GetVectorType() != VectorType::CONSTANT_VECTOR) {
      throw InvalidInputException("Can't use s2_format() with non-constant precision");
    }

    Execute(args.data[0], result, args.size(), precision.GetValue(0).GetValue<int8_t>());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count,
                             int8_t precision = -1) {
    GeographyDecoder decoder;
    s2geography::WKTWriter writer(precision);

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.kind == s2geography::GeographyKind::SHAPE_INDEX) {
            return StringVector::AddString(
                result, std::string("<S2ShapeIndex ") +
                            std::to_string(geog_str.GetSize()) + " b>");
          }
          auto geog = decoder.Decode(geog_str);
          std::string wkt = writer.write_feature(*geog);
          return StringVector::AddString(result, wkt);
        });
  }
};

struct S2GeogFromWKB {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_geogfromwkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkb", LogicalType::BLOB);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Converts a WKB blob to a geography.

The input WKB blog is assumed to have longitude/latitude coordinates and have
spherical edges. If edges are long and the input had a different edge type,
the resulting GEOGRAPHY may be invalid or represent a different location than
intended.
)");
          func.SetExample(R"(
SELECT s2_geogfromwkb(s2_aswkb(s2_data_city('Toronto'))) as geog;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });

    FunctionBuilder::RegisterScalar(
        loader, "s2_geogfromwkb_novalidate", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkb", LogicalType::BLOB);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFnNovalidate);
          });

          func.SetDescription(R"(
Returns the geography from a WKB blob skipping validation.

This is useful to determine which of some set of geometries is not valid and
why (or to help make them valid).
)");
          func.SetExample(R"(
SELECT s2_geogfromwkb_novalidate(
  s2_geogfromtext_novalidate('LINESTRING (0 0, 0 0, 1 1)').s2_aswkb()
);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void ExecuteFnNovalidate(DataChunk& args, ExpressionState& state,
                                         Vector& result) {
    s2geography::geoarrow::ImportOptions options;
    options.set_check(false);
    Execute(args.data[0], result, args.size(), options);
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count,
                             const s2geography::geoarrow::ImportOptions& options =
                                 s2geography::geoarrow::ImportOptions()) {
    s2geography::WKBReader reader(options);
    GeographyEncoder encoder;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkb) {
      std::unique_ptr<s2geography::Geography> geog =
          reader.ReadFeature(std::string_view(wkb.GetData(), wkb.GetSize()));
      return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
    });
  }
};

struct S2AsWKB {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(loader, "s2_aswkb", [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("geog", Types::GEOGRAPHY());
        variant.SetReturnType(LogicalType::BLOB);
        variant.SetFunction(ExecuteFn);
      });

      func.SetDescription(R"(
Serialize a GEOGRAPHY as well-known binary (WKB).

Note that because the internal representation of the GEOGRAPHY type is either
an S2_CELL_CENTER or a unit vector, WKB typically does not roundtrip through a
GEOGRAPHY.

The output contains spherical edges. If edges are large and the consumer does
not know that the edges are spherical, this may cause issues.
)");
      func.SetExample(R"(
SELECT s2_aswkb(s2_data_city('Toronto')) as wkb;
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "conversion");
    });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count,
                             const s2geography::geoarrow::ExportOptions& options =
                                 s2geography::geoarrow::ExportOptions()) {
    GeographyDecoder decoder;
    s2geography::WKBWriter writer;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkb) {
      std::unique_ptr<s2geography::Geography> geog = decoder.Decode(wkb);
      return StringVector::AddStringOrBlob(result, writer.WriteFeature(*geog));
    });
  }
};

struct S2GeogPrepare {
  static void Register(ExtensionLoader& loader) {
    FunctionBuilder::RegisterScalar(
        loader, "s2_prepare", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Prepares a geography for faster predicate and overlay operations.

For advanced users, this is useful for preparing input that will be subject
to a large number of intersection or containment checks. This high level terms,
this operation builds a cell-based index on the edges of the geography that
would otherwise have to occur on every intersection check.

This function returns its input for very small geographies (e.g., points)
that do not benefit from this operation.
)");
          func.SetExample(R"(
SELECT s2_prepare(s2_data_country('Fiji'));
----
CREATE TABLE countries AS
SELECT name, s2_prepare(geog) as geog
FROM s2_data_countries();

SELECT cities.name as city, countries.name as country
FROM s2_data_cities() AS cities
INNER JOIN countries ON s2_contains(countries.geog, cities.geog)
LIMIT 5;
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;
    GeographyEncoder encoder;

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          // For small geographies or something that is already prepared, don't
          // trigger a new index. 64 bytes is arbitrary here (should be tuned).
          if (decoder.tag.kind == s2geography::GeographyKind::SHAPE_INDEX ||
              geog_str.GetSize() < 64) {
            // Maybe a way to avoid copying geog_str?
            return StringVector::AddStringOrBlob(result, geog_str);
          }

          std::unique_ptr<s2geography::Geography> geog = decoder.Decode(geog_str);
          s2geography::ShapeIndexGeography index_geog(*geog);
          return StringVector::AddStringOrBlob(result, encoder.Encode(index_geog));
        });
  }
};

void ImportWKBToGeography(Vector& source, Vector& result, idx_t count,
                          const s2geography::geoarrow::ImportOptions& options) {
  S2GeogFromWKB::Execute(source, result, count, options);
}

void ExportGeographyToWKB(Vector& source, Vector& result, idx_t count,
                          const s2geography::geoarrow::ExportOptions& options) {
  S2AsWKB::Execute(source, result, count, options);
}

void RegisterS2GeographyFunctionsIO(ExtensionLoader& loader) {
  S2GeogFromText::Register(loader);
  S2GeogFromWKB::Register(loader);
  S2AsText::Register(loader);
  S2AsWKB::Register(loader);
  S2GeogPrepare::Register(loader);
}

}  // namespace duckdb_s2
}  // namespace duckdb
