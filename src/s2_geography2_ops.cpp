#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "s2_functions_io.hpp"
#include "s2_types.hpp"
#include "s2geography/wkb.h"
#include "s2geography/wkt-reader.h"
#include "s2geography/wkt-writer.h"
#include "s2geography_c.h"

namespace duckdb {
namespace duckdb_s2 {
namespace {

class CApiContext {
 public:
  CApiContext() {
    Check(S2GeogErrorCreate(&error_));
    Check(S2GeogFactoryCreate(&factory_));
    Check(S2GeogCreate(&lhs_));
    Check(S2GeogCreate(&rhs_));
  }

  ~CApiContext() {
    if (rhs_) S2GeogDestroy(rhs_);
    if (lhs_) S2GeogDestroy(lhs_);
    if (factory_) S2GeogFactoryDestroy(factory_);
    if (error_) S2GeogErrorDestroy(error_);
  }

  CApiContext(const CApiContext&) = delete;
  CApiContext& operator=(const CApiContext&) = delete;

  void Init(S2Geog* out, string_t wkb) {
    Check(S2GeogFactoryInitFromWkbNonOwning(
        factory_, reinterpret_cast<const uint8_t*>(wkb.GetData()), wkb.GetSize(), out,
        error_));
  }

  bool Eval(int op_id, string_t lhs, string_t rhs) {
    Init(lhs_, lhs);
    Init(rhs_, rhs);
    S2GeogOp* op = nullptr;
    Check(S2GeogOpCreate(&op, op_id));
    try {
      Check(S2GeogOpEvalGeogGeog(op, lhs_, rhs_, error_));
      const bool value = S2GeogOpGetInt(op) != 0;
      S2GeogOpDestroy(op);
      return value;
    } catch (...) {
      if (op) S2GeogOpDestroy(op);
      throw;
    }
  }

  bool Eval(int op_id, string_t lhs, string_t rhs, double arg) {
    Init(lhs_, lhs);
    Init(rhs_, rhs);
    S2GeogOp* op = nullptr;
    Check(S2GeogOpCreate(&op, op_id));
    try {
      Check(S2GeogOpEvalGeogGeogDouble(op, lhs_, rhs_, arg, error_));
      const bool value = S2GeogOpGetInt(op) != 0;
      S2GeogOpDestroy(op);
      return value;
    } catch (...) {
      if (op) S2GeogOpDestroy(op);
      throw;
    }
  }

 private:
  void Check(S2GeogErrorCode code) {
    if (code != S2GEOGRAPHY_OK) {
      throw InvalidInputException("s2geography C API error: %s",
                                  S2GeogErrorGetMessage(error_));
    }
  }

  S2GeogError* error_ = nullptr;
  S2GeogFactory* factory_ = nullptr;
  S2Geog* lhs_ = nullptr;
  S2Geog* rhs_ = nullptr;
};

static void FromText(DataChunk& args, ExpressionState&, Vector& result) {
  auto& source = args.data[0];
  const auto count = args.size();
  s2geography::WKTReader reader;
  s2geography::WKBWriter writer;
  UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkt) {
    auto geog = reader.read_feature(wkt.GetData(), wkt.GetSize());
    return StringVector::AddStringOrBlob(result, writer.WriteFeature(*geog));
  });
}

static void FromWkb(DataChunk& args, ExpressionState&, Vector& result) {
  CApiContext context;
  S2Geog* scratch = nullptr;
  if (S2GeogCreate(&scratch) != S2GEOGRAPHY_OK) {
    throw OutOfMemoryException("Could not allocate an s2geography object");
  }
  try {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(), [&](string_t wkb) {
          context.Init(scratch, wkb);
          return StringVector::AddStringOrBlob(result, wkb);
        });
    S2GeogDestroy(scratch);
  } catch (...) {
    S2GeogDestroy(scratch);
    throw;
  }
}

static void AsText(DataChunk& args, ExpressionState&, Vector& result) {
  ExportWKBToWKT(args.data[0], result, args.size());
}

static void AsWkb(DataChunk& args, ExpressionState&, Vector& result) {
  UnaryExecutor::Execute<string_t, string_t>(
      args.data[0], result, args.size(),
      [&](string_t wkb) { return StringVector::AddStringOrBlob(result, wkb); });
}

template <int OP_ID>
static void BinaryPredicate(DataChunk& args, ExpressionState&, Vector& result) {
  CApiContext context;
  BinaryExecutor::Execute<string_t, string_t, bool>(
      args.data[0], args.data[1], result, args.size(),
      [&](string_t lhs, string_t rhs) { return context.Eval(OP_ID, lhs, rhs); });
}

static void DWithin(DataChunk& args, ExpressionState&, Vector& result) {
  CApiContext context;
  TernaryExecutor::Execute<string_t, string_t, double, bool>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](string_t lhs, string_t rhs, double distance) {
        return context.Eval(S2GEOGRAPHY_OP_DISTANCE_WITHIN, lhs, rhs, distance);
      });
}

static bool CastFromText(Vector& source, Vector& result, idx_t count, CastParameters&) {
  s2geography::WKTReader reader;
  s2geography::WKBWriter writer;
  UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkt) {
    auto geog = reader.read_feature(wkt.GetData(), wkt.GetSize());
    return StringVector::AddStringOrBlob(result, writer.WriteFeature(*geog));
  });
  return true;
}

void RegisterUnary(ExtensionLoader& loader, const char* name, LogicalType input,
                   LogicalType output, scalar_function_t fn) {
  loader.RegisterFunction(
      ScalarFunction(name, {std::move(input)}, std::move(output), fn));
}

void RegisterBinaryPredicate(ExtensionLoader& loader, const char* name,
                             scalar_function_t fn) {
  loader.RegisterFunction(ScalarFunction(name, {Types::GEOGRAPHY2(), Types::GEOGRAPHY2()},
                                         LogicalType::BOOLEAN, fn));
}

}  // namespace

void RegisterS2Geography2Ops(ExtensionLoader& loader) {
  RegisterUnary(loader, "s2_geogfromtext2", LogicalType::VARCHAR, Types::GEOGRAPHY2(),
                FromText);
  RegisterUnary(loader, "s2_geogfromwkb2", LogicalType::BLOB, Types::GEOGRAPHY2(),
                FromWkb);
  RegisterUnary(loader, "s2_astext", Types::GEOGRAPHY2(), LogicalType::VARCHAR, AsText);
  RegisterUnary(loader, "s2_aswkb", Types::GEOGRAPHY2(), LogicalType::BLOB, AsWkb);

  RegisterBinaryPredicate(loader, "s2_intersects",
                          BinaryPredicate<S2GEOGRAPHY_OP_INTERSECTS>);
  RegisterBinaryPredicate(loader, "s2_contains",
                          BinaryPredicate<S2GEOGRAPHY_OP_CONTAINS>);
  RegisterBinaryPredicate(loader, "s2_within", BinaryPredicate<S2GEOGRAPHY_OP_WITHIN>);
  RegisterBinaryPredicate(loader, "s2_equals", BinaryPredicate<S2GEOGRAPHY_OP_EQUALS>);
  RegisterBinaryPredicate(loader, "s2_disjoint",
                          BinaryPredicate<S2GEOGRAPHY_OP_DISJOINT>);
  loader.RegisterFunction(ScalarFunction(
      "s2_dwithin", {Types::GEOGRAPHY2(), Types::GEOGRAPHY2(), LogicalType::DOUBLE},
      LogicalType::BOOLEAN, DWithin));

  loader.RegisterCastFunction(LogicalType::VARCHAR, Types::GEOGRAPHY2(),
                              BoundCastInfo(CastFromText), 1);
}

}  // namespace duckdb_s2
}  // namespace duckdb
