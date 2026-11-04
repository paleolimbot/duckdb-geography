cmake_minimum_required(VERSION 3.5)

include(FetchContent)

# Set extension name here
set(TARGET_NAME geography)

# Required for S2. CACHE ... FORCE so DuckDB's own build picks it up too: DuckDB
# configures C++11, where the static constexpr class-type member
# duckdb::BufferedFileWriter::DEFAULT_OPEN_FLAGS is not implicitly inline, and
# GCC 14 on aarch64 then rejects the duplicate when linking
# tools/plan_serializer (duckdb/duckdb#22097). That tool arrived in DuckDB
# v1.5.2, which is exactly when this extension stopped building.
set(CMAKE_CXX_STANDARD
    17
    CACHE STRING "C++ standard to enforce" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(absl REQUIRED)
find_package(s2 CONFIG REQUIRED)

# S2's installed CMake config does not publish version variables. Keep these in
# sync with the vcpkg s2geometry dependency for s2geography's compatibility
# definitions and the extension's dependency metadata.
set(S2_VERSION_MAJOR 0)
set(S2_VERSION_MINOR 13)
set(S2_VERSION_PATCH 1)

FetchContent_Declare(
  nanoarrow
  URL https://github.com/apache/arrow-nanoarrow/archive/refs/tags/apache-arrow-nanoarrow-0.7.0.zip
  URL_HASH
    SHA256=bd40a2ee849c650b243818b12e925cac17f8fc300acacd141177345fd6b2e9f4)

FetchContent_Declare(
  geoarrow
  URL https://github.com/geoarrow/geoarrow-c/archive/e4ba826fd8e4012bf67b27c2cb64313a78627ed8.zip
  URL_HASH
    SHA256=398874eb854cfcb5afa594bc4b48be0d2f9cf1937ed9d4bf59657bd4cbdeb396)

# Build s2geography
set(BUILD_SHARED_LIBS OFF)
add_subdirectory(s2geography)
target_compile_definitions(
  s2geography PRIVATE -DGEOARROW_FAST_FLOAT_NAMESPACE=duckdb_fast_float)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(s2geography
                         PRIVATE $<$<CONFIG:Debug>:-fno-sanitize=undefined>)
  target_compile_options(s2geography_c
                         PRIVATE $<$<CONFIG:Debug>:-fno-sanitize=undefined>)
  target_compile_definitions(s2geography
                             PRIVATE $<$<CONFIG:Debug>:NDEBUG_SANITIZER>)
  target_compile_definitions(s2geography_c
                             PRIVATE $<$<CONFIG:Debug>:NDEBUG_SANITIZER>)
endif()

# Keep the extension's GeoArrow and nanoarrow symbols separate from the vendored
# copies used internally by s2geography. In particular, s2geography must be
# configured before these targets exist or it will link against their
# differently namespaced ABI instead of compiling its own implementation.
set(NANOARROW_NAMESPACE DuckDBGeography)
set(GEOARROW_NAMESPACE DuckDBGeography)
FetchContent_MakeAvailable(nanoarrow)
FetchContent_MakeAvailable(geoarrow)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/geography_extension.cpp
    src/function_builder.cpp
    src/s2_dependencies.cpp
    src/s2_types.cpp
    src/s2_cell_ops.cpp
    src/s2_functions_io.cpp
    src/s2_binary_index_ops.cpp
    src/s2_data.cpp
    src/s2_accessors.cpp
    src/s2_bounds.cpp
    src/s2_geoarrow.cpp)
list(APPEND EXTENSION_SOURCES src/s2_geography2_ops.cpp)

# Workaround for difference between v1.1.3 and main with respect to
# FunctionEntry fields
if(DUCKDB_VERSION STREQUAL "v1.1.3")
  add_definitions(-DDUCKDB_FUNC_ENTRY_HAS_METADATA=1)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(${EXTENSION_NAME}
                         PRIVATE $<$<CONFIG:Debug>:-fno-sanitize=undefined>)
  target_compile_options(${LOADABLE_EXTENSION_NAME}
                         PRIVATE $<$<CONFIG:Debug>:-fno-sanitize=undefined>)
  target_compile_definitions(${EXTENSION_NAME}
                             PRIVATE $<$<CONFIG:Debug>:NDEBUG_SANITIZER>)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME}
                             PRIVATE $<$<CONFIG:Debug>:NDEBUG_SANITIZER>)
endif()

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(
  ${EXTENSION_NAME}
  s2geography_c
  s2geography
  s2::s2
  OpenSSL::SSL
  OpenSSL::Crypto
  geoarrow::geoarrow
  nanoarrow::nanoarrow)
target_link_libraries(
  ${LOADABLE_EXTENSION_NAME}
  s2geography_c
  s2geography
  s2::s2
  OpenSSL::SSL
  OpenSSL::Crypto
  geoarrow::geoarrow
  nanoarrow::nanoarrow)

install(
  TARGETS ${EXTENSION_NAME} s2geography_c s2geography
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
