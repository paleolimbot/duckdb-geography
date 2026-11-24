#pragma once

#include "duckdb.hpp"
#include "s2geography/sedona_udf/sedona_extension.h"

namespace duckdb {
namespace duckdb_s2 {

using ArrowScalarKernelInit = void (*)(SedonaCScalarKernel*);

// Execute a Sedona Arrow scalar kernel over a DuckDB batch. Constants are passed
// as length-one arrays; other vector encodings are handled by ArrowConverter.
// GEOGRAPHY uses spherical geoarrow.wkb metadata, independent of the database's
// Arrow extension registrations. Returned vectors retain their Arrow buffers.
void ExecuteArrowScalar(ArrowScalarKernelInit init, ClientContext& context,
                        DataChunk& args, Vector& result);

template <ArrowScalarKernelInit INIT>
void ArrowScalarFunction(DataChunk& args, ExpressionState& state, Vector& result) {
  ExecuteArrowScalar(INIT, state.GetContext(), args, result);
}

}  // namespace duckdb_s2
}  // namespace duckdb
