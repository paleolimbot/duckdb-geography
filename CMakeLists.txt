cmake_minimum_required(VERSION 3.5)

include(FetchContent)

# Set extension name here
set(TARGET_NAME geography)

# Required for S2
set(CMAKE_CXX_STANDARD 17)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(absl REQUIRED)

set(NANOARROW_NAMESPACE DuckDBGeography)
FetchContent_Declare(
  nanoarrow
  URL https://github.com/apache/arrow-nanoarrow/archive/refs/tags/apache-arrow-nanoarrow-0.6.0.zip
  URL_HASH
    SHA256=73b3235453902c9e92b5e942683d02d764a2aea1f999c03bf4a747c90df2f505)

set(GEOARROW_NAMESPACE DuckDBGeography)
FetchContent_Declare(
  geoarrow
  URL https://github.com/geoarrow/geoarrow-c/archive/791682927ab8541be189b7dbe673edeb478a0bae.zip
  URL_HASH
    SHA256=7c529a5224745b4b7fd00b677e41d4ef47a7c2c72f6fa337318193f656fd4ef2)

FetchContent_MakeAvailable(nanoarrow)
FetchContent_MakeAvailable(geoarrow)

# S2's CMake is pretty awful so we just build the library ourselves
add_library(
  s2
  s2geometry/src/s2/encoded_s2cell_id_vector.cc
  s2geometry/src/s2/encoded_s2point_vector.cc
  s2geometry/src/s2/encoded_s2shape_index.cc
  s2geometry/src/s2/encoded_string_vector.cc
  s2geometry/src/s2/id_set_lexicon.cc
  s2geometry/src/s2/mutable_s2shape_index.cc
  s2geometry/src/s2/r2rect.cc
  s2geometry/src/s2/s1angle.cc
  s2geometry/src/s2/s1chord_angle.cc
  s2geometry/src/s2/s1interval.cc
  s2geometry/src/s2/s2boolean_operation.cc
  s2geometry/src/s2/s2buffer_operation.cc
  s2geometry/src/s2/s2builder.cc
  s2geometry/src/s2/s2builder_graph.cc
  s2geometry/src/s2/s2builderutil_closed_set_normalizer.cc
  s2geometry/src/s2/s2builderutil_find_polygon_degeneracies.cc
  s2geometry/src/s2/s2builderutil_get_snapped_winding_delta.cc
  s2geometry/src/s2/s2builderutil_lax_polygon_layer.cc
  s2geometry/src/s2/s2builderutil_lax_polyline_layer.cc
  s2geometry/src/s2/s2builderutil_s2point_vector_layer.cc
  s2geometry/src/s2/s2builderutil_s2polygon_layer.cc
  s2geometry/src/s2/s2builderutil_s2polyline_layer.cc
  s2geometry/src/s2/s2builderutil_s2polyline_vector_layer.cc
  s2geometry/src/s2/s2builderutil_snap_functions.cc
  s2geometry/src/s2/s2cap.cc
  s2geometry/src/s2/s2cell.cc
  s2geometry/src/s2/s2cell_id.cc
  s2geometry/src/s2/s2cell_index.cc
  s2geometry/src/s2/s2cell_union.cc
  s2geometry/src/s2/s2centroids.cc
  s2geometry/src/s2/s2closest_cell_query.cc
  s2geometry/src/s2/s2closest_edge_query.cc
  s2geometry/src/s2/s2closest_point_query.cc
  s2geometry/src/s2/s2contains_vertex_query.cc
  s2geometry/src/s2/s2convex_hull_query.cc
  s2geometry/src/s2/s2coords.cc
  s2geometry/src/s2/s2crossing_edge_query.cc
  s2geometry/src/s2/s2debug.cc
  s2geometry/src/s2/s2earth.cc
  s2geometry/src/s2/s2edge_clipping.cc
  s2geometry/src/s2/s2edge_crosser.cc
  s2geometry/src/s2/s2edge_crossings.cc
  s2geometry/src/s2/s2edge_distances.cc
  s2geometry/src/s2/s2edge_tessellator.cc
  s2geometry/src/s2/s2error.cc
  s2geometry/src/s2/s2furthest_edge_query.cc
  s2geometry/src/s2/s2hausdorff_distance_query.cc
  s2geometry/src/s2/s2latlng.cc
  s2geometry/src/s2/s2latlng_rect.cc
  s2geometry/src/s2/s2latlng_rect_bounder.cc
  s2geometry/src/s2/s2lax_loop_shape.cc
  s2geometry/src/s2/s2lax_polygon_shape.cc
  s2geometry/src/s2/s2lax_polyline_shape.cc
  s2geometry/src/s2/s2loop.cc
  s2geometry/src/s2/s2loop_measures.cc
  s2geometry/src/s2/s2measures.cc
  s2geometry/src/s2/s2memory_tracker.cc
  s2geometry/src/s2/s2metrics.cc
  s2geometry/src/s2/s2max_distance_targets.cc
  s2geometry/src/s2/s2min_distance_targets.cc
  s2geometry/src/s2/s2padded_cell.cc
  s2geometry/src/s2/s2point_compression.cc
  s2geometry/src/s2/s2point_region.cc
  s2geometry/src/s2/s2pointutil.cc
  s2geometry/src/s2/s2polygon.cc
  s2geometry/src/s2/s2polyline.cc
  s2geometry/src/s2/s2polyline_alignment.cc
  s2geometry/src/s2/s2polyline_measures.cc
  s2geometry/src/s2/s2polyline_simplifier.cc
  s2geometry/src/s2/s2predicates.cc
  s2geometry/src/s2/s2projections.cc
  s2geometry/src/s2/s2r2rect.cc
  s2geometry/src/s2/s2region.cc
  s2geometry/src/s2/s2region_term_indexer.cc
  s2geometry/src/s2/s2region_coverer.cc
  s2geometry/src/s2/s2region_intersection.cc
  s2geometry/src/s2/s2region_union.cc
  s2geometry/src/s2/s2shape_index.cc
  s2geometry/src/s2/s2shape_index_buffered_region.cc
  s2geometry/src/s2/s2shape_index_measures.cc
  s2geometry/src/s2/s2shape_measures.cc
  s2geometry/src/s2/s2shape_nesting_query.cc
  s2geometry/src/s2/s2shapeutil_build_polygon_boundaries.cc
  s2geometry/src/s2/s2shapeutil_coding.cc
  s2geometry/src/s2/s2shapeutil_contains_brute_force.cc
  s2geometry/src/s2/s2shapeutil_conversion.cc
  s2geometry/src/s2/s2shapeutil_edge_iterator.cc
  s2geometry/src/s2/s2shapeutil_get_reference_point.cc
  s2geometry/src/s2/s2shapeutil_visit_crossing_edge_pairs.cc
  s2geometry/src/s2/s2text_format.cc
  s2geometry/src/s2/s2wedge_relations.cc
  s2geometry/src/s2/s2winding_operation.cc
  s2geometry/src/s2/util/bits/bit-interleave.cc
  s2geometry/src/s2/util/coding/coder.cc
  s2geometry/src/s2/util/coding/varint.cc
  s2geometry/src/s2/util/math/exactfloat/exactfloat.cc
  s2geometry/src/s2/util/math/mathutil.cc
  s2geometry/src/s2/util/units/length-units.cc)

if(WIN32 AND NOT MSVC)
  target_compile_definitions(s2 PUBLIC _USE_MATH_DEFINES)
  set(S2_EXTRA_OPENSSL_LIBS crypt32 z ws2_32 gdi32 crypt32)
elseif(MSVC)
  target_compile_definitions(s2 PUBLIC NOMINMAX _USE_MATH_DEFINES)
  target_compile_options(s2 PUBLIC /J)
else()
  # target_compile_definitions(s2 PRIVATE -Wno-attributes
  # -Wno-deprecated-declarations "-Wno-comment" -Wno-pedantic )
endif()

# HACK ALERT *** HACK ALERT *** HACK ALERT s2geometry doesn't bother to
# namespace any of their symbols, and neither does libfsst. Both of them define
# an "Encoder" :facepalm:. This seems to work.
target_compile_definitions(s2 PUBLIC Encoder=S2Encoder)

target_include_directories(
  s2 PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/s2geometry/src>)
target_link_libraries(
  s2
  ${OPENSSL_LIBRARIES}
  absl::base
  absl::btree
  absl::check
  absl::config
  absl::core_headers
  absl::dynamic_annotations
  absl::endian
  absl::fixed_array
  absl::flags
  absl::flat_hash_map
  absl::flat_hash_set
  absl::hash
  absl::inlined_vector
  absl::int128
  absl::log
  absl::log_severity
  absl::memory
  absl::span
  absl::status
  absl::str_format
  absl::strings
  absl::type_traits
  absl::utility
  ${CMAKE_THREAD_LIBS_INIT})

# Build s2geography
add_library(s2::s2 ALIAS s2)
set(S2_VERSION_MAJOR 0)
set(S2_VERSION_MINOR 11)
set(S2_VERSION_PATCH 1)
set(BUILD_SHARED_LIBS OFF)
add_subdirectory(s2geography)
target_compile_definitions(
  s2geography PRIVATE -DGEOARROW_FAST_FLOAT_NAMESPACE=duckdb_fast_float)

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
    src/s2_bounds.cpp)

# Workaround for difference between v1.1.3 and main with respect to
# FunctionEntry fields
if(DUCKDB_VERSION STREQUAL "v1.1.3")
  add_definitions(-DDUCKDB_FUNC_ENTRY_HAS_METADATA=1)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(
  ${EXTENSION_NAME}
  s2geography
  s2
  OpenSSL::SSL
  OpenSSL::Crypto
  ${S2_EXTRA_OPENSSL_LIBS}
  geoarrow::geoarrow
  nanoarrow::nanoarrow)
target_link_libraries(
  ${LOADABLE_EXTENSION_NAME}
  s2geography
  s2
  OpenSSL::SSL
  OpenSSL::Crypto
  ${S2_EXTRA_OPENSSL_LIBS}
  geoarrow::geoarrow
  nanoarrow::nanoarrow)

install(
  TARGETS ${EXTENSION_NAME} s2geography s2
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
