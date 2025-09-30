#include "s2_types.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace duckdb_s2 {
LogicalType Types::S2_CELL() {
  LogicalType type = LogicalType::UBIGINT;
  type.SetAlias("S2_CELL");
  return type;
}

LogicalType Types::S2_CELL_UNION() {
  LogicalType type = LogicalType::LIST(S2_CELL());
  type.SetAlias("S2_CELL_UNION");
  return type;
}

LogicalType Types::S2_CELL_CENTER() {
  LogicalType type = LogicalType::UBIGINT;
  type.SetAlias("S2_CELL_CENTER");
  return type;
}

LogicalType Types::GEOGRAPHY() {
  LogicalType type = LogicalType::BLOB;
  type.SetAlias("GEOGRAPHY");
  return type;
}

LogicalType Types::S2_BOX() {
  LogicalType type = LogicalType::STRUCT({{"xmin", LogicalType::DOUBLE},
                                          {"ymin", LogicalType::DOUBLE},
                                          {"xmax", LogicalType::DOUBLE},
                                          {"ymax", LogicalType::DOUBLE}});
  type.SetAlias("S2_BOX");
  return type;
}

void RegisterTypes(ExtensionLoader& loader) {
  loader.RegisterType("S2_CELL", Types::S2_CELL());
  loader.RegisterType("S2_CELL_UNION", Types::S2_CELL_UNION());
  loader.RegisterType("S2_CELL_CENTER", Types::S2_CELL_CENTER());
  loader.RegisterType("GEOGRAPHY", Types::GEOGRAPHY());
  loader.RegisterType("S2_BOX", Types::S2_BOX());
}

}  // namespace duckdb_s2
}  // namespace duckdb
