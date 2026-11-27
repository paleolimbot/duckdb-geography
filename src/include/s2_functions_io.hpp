
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace duckdb_s2 {
void ImportWKBToGeography(const Vector& source, Vector& result, idx_t count);

void ExportGeographyToWKB(const Vector& source, Vector& result, idx_t count);

void ImportWKTToWKB(Vector& source, Vector& result, idx_t count);

void ExportWKBToWKT(const Vector& source, Vector& result, idx_t count,
                    int precision = 16);

void RegisterS2GeographyFunctionsIO(ExtensionLoader& loader);
}  // namespace duckdb_s2
}  // namespace duckdb
