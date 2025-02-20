#pragma once

#include "duckdb/main/database.hpp"

#include "s2_functions_io.hpp"

namespace duckdb {

namespace duckdb_s2 {

void RegisterS2GeographyPredicates(DatabaseInstance& instance);
void RegisterS2GeographyAccessors(DatabaseInstance& instance);
void RegisterS2GeographyBounds(DatabaseInstance& instance);
void RegisterGeoArrowExtensions(DatabaseInstance& instance);

inline void RegisterS2GeographyOps(DatabaseInstance& instance) {
  RegisterS2GeographyFunctionsIO(instance);
  RegisterS2GeographyPredicates(instance);
  RegisterS2GeographyAccessors(instance);
  RegisterS2GeographyBounds(instance);
  RegisterGeoArrowExtensions(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
