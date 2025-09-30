#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace duckdb_s2 {

void RegisterS2CellOps(ExtensionLoader& loader);

}
}  // namespace duckdb
