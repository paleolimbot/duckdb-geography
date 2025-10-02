
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"

#include "s2geography/geoarrow.h"

namespace duckdb {
namespace duckdb_s2 {
void ImportWKBToGeography(Vector& source, Vector& result, idx_t count,
                          const s2geography::geoarrow::ImportOptions& options =
                              s2geography::geoarrow::ImportOptions());

void ExportGeographyToWKB(Vector& source, Vector& result, idx_t count,
                          const s2geography::geoarrow::ExportOptions& options =
                              s2geography::geoarrow::ExportOptions());

void RegisterS2GeographyFunctionsIO(ExtensionLoader& loader);
}  // namespace duckdb_s2
}  // namespace duckdb
