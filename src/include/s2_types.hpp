#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace duckdb_s2 {

struct Types {
  static LogicalType S2_CELL();
  static LogicalType S2_CELL_UNION();
  static LogicalType S2_CELL_CENTER();
  static LogicalType GEOGRAPHY();
  static LogicalType S2_BOX();
};

void RegisterTypes(ExtensionLoader& loader);

}  // namespace duckdb_s2
}  // namespace duckdb
