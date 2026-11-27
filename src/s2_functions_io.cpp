
#include "duckdb/main/database.hpp"

#include "s2_functions_io.hpp"
#include "s2_types.hpp"
#include "s2_wkb.hpp"

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
    Execute(args.data[0], result, args.size(), false);
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(const Vector& source, Vector& result, idx_t count,
                             bool validate = true) {
    WKTToWKB converter;
    WKBGeography reader;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkt) {
      auto wkb = converter.Convert(wkt.GetData(), wkt.GetSize());
      if (validate) {
        S2Error error;
        const auto& geography = reader.Read(string_t(wkb.data(), wkb.size()));
        if (FindValidationError(geography, &error)) {
          throw InvalidInputException("Invalid geography: %s",
                                      std::string(error.message()));
        }
      }
      return StringVector::AddStringOrBlob(result, wkb);
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

  static inline void Execute(const Vector& source, Vector& result, idx_t count,
                             int8_t precision = -1) {
    ExportWKBToWKT(source, result, count, precision < 0 ? 16 : precision);
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
    Execute(args.data[0], result, args.size(), false);
  }

  static inline void Execute(const Vector& source, Vector& result, idx_t count,
                             bool validate = true) {
    WKBGeography reader;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkb) {
      const auto& geography = reader.Read(wkb);
      if (validate) {
        S2Error error;
        if (FindValidationError(geography, &error)) {
          throw InvalidInputException("Invalid geography: %s",
                                      std::string(error.message()));
        }
      }
      return StringVector::AddStringOrBlob(result, wkb);
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

GEOGRAPHY uses WKB as its native representation, so this operation returns the
underlying bytes unchanged.

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

  static inline void Execute(const Vector& source, Vector& result, idx_t count,
                             bool copy = true) {
    (void)copy;
    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkb) {
      return StringVector::AddStringOrBlob(result, wkb);
    });
  }
};

void ImportWKBToGeography(const Vector& source, Vector& result, idx_t count) {
  S2GeogFromWKB::Execute(source, result, count);
}

void ExportGeographyToWKB(const Vector& source, Vector& result, idx_t count) {
  S2AsWKB::Execute(source, result, count);
}

void RegisterS2GeographyFunctionsIO(ExtensionLoader& loader) {
  S2GeogFromText::Register(loader);
  S2GeogFromWKB::Register(loader);
  S2AsText::Register(loader);
  S2AsWKB::Register(loader);
}

}  // namespace duckdb_s2
}  // namespace duckdb
