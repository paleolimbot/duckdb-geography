#pragma once

#include <cstdint>
#include <cstring>

#include "duckdb/common/bswap.hpp"

namespace duckdb {
namespace duckdb_s2 {

// Compile-time check: ensure we're on a little-endian platform
// DuckDB only officially supports little-endian architectures (x86_64, ARM64)
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "duckdb-geography only supports little-endian platforms");

// Helper functions for endian-aware data access
// These replace s2geometry's LittleEndian/BigEndian classes which were removed in v0.12+
// and now depend on newer Abseil versions not available in DuckDB CI

inline double LoadDoubleLE(const void* p) {
  double result;
  std::memcpy(&result, p, sizeof(double));
  return result;
}

inline double LoadDoubleBE(const void* p) {
  uint64_t val;
  std::memcpy(&val, p, sizeof(uint64_t));
  val = BSWAP64(val);
  double result;
  std::memcpy(&result, &val, sizeof(double));
  return result;
}

inline uint32_t LoadUInt32BE(const void* p) {
  uint32_t val;
  std::memcpy(&val, p, sizeof(uint32_t));
  return BSWAP32(val);
}

inline void StoreDoubleLE(void* dest, double value) {
  std::memcpy(dest, &value, sizeof(double));
}

}  // namespace duckdb_s2
}  // namespace duckdb
