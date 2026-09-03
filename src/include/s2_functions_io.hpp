
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace duckdb_s2 {
void ImportWKBToGeography(Vector& source, Vector& result, idx_t count);

void ExportGeographyToWKB(Vector& source, Vector& result, idx_t count);

void RegisterS2GeographyFunctionsIO(ExtensionLoader& loader);
}  // namespace duckdb_s2
}  // namespace duckdb
