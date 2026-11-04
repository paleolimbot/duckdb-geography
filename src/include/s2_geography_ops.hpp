#pragma once

#include "duckdb/main/database.hpp"

#include "s2_functions_io.hpp"

namespace duckdb {

namespace duckdb_s2 {

void RegisterS2GeographyPredicates(ExtensionLoader& loader);
void RegisterS2GeographyAccessors(ExtensionLoader& loader);
void RegisterS2GeographyBounds(ExtensionLoader& loader);
void RegisterGeoArrowExtensions(ExtensionLoader& loader);
void RegisterS2Geography2Ops(ExtensionLoader& loader);

inline void RegisterS2GeographyOps(ExtensionLoader& loader) {
  RegisterS2GeographyFunctionsIO(loader);
  RegisterS2GeographyPredicates(loader);
  RegisterS2GeographyAccessors(loader);
  RegisterS2GeographyBounds(loader);
  RegisterGeoArrowExtensions(loader);
  RegisterS2Geography2Ops(loader);
}

}  // namespace duckdb_s2
}  // namespace duckdb
