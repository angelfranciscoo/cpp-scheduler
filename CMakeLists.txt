# Copyright 2026. SPDX-License-Identifier: MIT
cmake_minimum_required(VERSION 3.16)

project(rtsched
  VERSION 1.0.0
  DESCRIPTION "Real-time priority task scheduler with deadline-aware state machine lifecycle"
  LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

# Only build tests/benchmarks when this is the top-level project, so that
# add_subdirectory() consumers do not inherit them.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(RTSCHED_IS_TOP_LEVEL ON)
else()
  set(RTSCHED_IS_TOP_LEVEL OFF)
endif()

option(RTSCHED_BUILD_TESTS      "Build unit and stress tests"        ${RTSCHED_IS_TOP_LEVEL})
option(RTSCHED_BUILD_BENCHMARKS "Build the benchmark suite"          ${RTSCHED_IS_TOP_LEVEL})
option(RTSCHED_BUILD_EXAMPLES   "Build the usage example"            ${RTSCHED_IS_TOP_LEVEL})
option(RTSCHED_WERROR           "Treat warnings as errors"           OFF)
option(RTSCHED_ENABLE_TSAN      "Build with ThreadSanitizer"         OFF)
option(RTSCHED_ENABLE_ASAN      "Build with AddressSanitizer"        OFF)
option(RTSCHED_ENABLE_UBSAN     "Build with UndefinedBehaviorSanitizer" OFF)
option(RTSCHED_USE_SYSTEM_GTEST "Use an installed GoogleTest instead of fetching it" ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Benchmarks are meaningless in a debug build; default to something optimized.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
  message(STATUS "rtsched: CMAKE_BUILD_TYPE not set, defaulting to RelWithDebInfo")
endif()

# ---------------------------------------------------------------------------
# Shared compile settings
# ---------------------------------------------------------------------------

add_library(rtsched_options INTERFACE)

if(MSVC)
  target_compile_options(rtsched_options INTERFACE /W4 /permissive-)
  if(RTSCHED_WERROR)
    target_compile_options(rtsched_options INTERFACE /WX)
  endif()
else()
  target_compile_options(rtsched_options INTERFACE
    -Wall -Wextra -Wpedantic
    -Wshadow                  # a shadowed lock guard is a real bug class
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Wcast-align
    -Wunused
    -Woverloaded-virtual
    -Wconversion -Wsign-conversion
    -Wdouble-promotion
    -Wformat=2)
  if(RTSCHED_WERROR)
    target_compile_options(rtsched_options INTERFACE -Werror)
  endif()
endif()

# Sanitizers. TSan is the important one here: it is the only practical way to
# prove the absence of the data races this design is built to avoid. ASan and
# TSan cannot be combined.
set(RTSCHED_SANITIZERS "")
if(RTSCHED_ENABLE_TSAN AND RTSCHED_ENABLE_ASAN)
  message(FATAL_ERROR "rtsched: ASan and TSan are mutually exclusive")
endif()
if(RTSCHED_ENABLE_TSAN)
  list(APPEND RTSCHED_SANITIZERS thread)
endif()
if(RTSCHED_ENABLE_ASAN)
  list(APPEND RTSCHED_SANITIZERS address)
endif()
if(RTSCHED_ENABLE_UBSAN)
  list(APPEND RTSCHED_SANITIZERS undefined)
endif()
if(RTSCHED_SANITIZERS)
  string(REPLACE ";" "," RTSCHED_SANITIZER_LIST "${RTSCHED_SANITIZERS}")
  message(STATUS "rtsched: sanitizers enabled: ${RTSCHED_SANITIZER_LIST}")
  target_compile_options(rtsched_options INTERFACE
    -fsanitize=${RTSCHED_SANITIZER_LIST} -fno-omit-frame-pointer -g)
  target_link_options(rtsched_options INTERFACE -fsanitize=${RTSCHED_SANITIZER_LIST})
endif()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

add_library(rtsched STATIC
  src/task.cpp
  src/metrics.cpp
  src/scheduler.cpp)

target_include_directories(rtsched PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)

target_link_libraries(rtsched PUBLIC rtsched_options)

find_package(Threads REQUIRED)
target_link_libraries(rtsched PUBLIC Threads::Threads)

set_target_properties(rtsched PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  VERSION ${PROJECT_VERSION})

add_library(rtsched::rtsched ALIAS rtsched)

# ---------------------------------------------------------------------------
# GoogleTest
# ---------------------------------------------------------------------------

if(RTSCHED_BUILD_TESTS)
  set(RTSCHED_GTEST_TARGETS "")
  if(RTSCHED_USE_SYSTEM_GTEST)
    find_package(GTest QUIET)
    if(GTest_FOUND)
      set(RTSCHED_GTEST_TARGETS GTest::gtest_main)
      message(STATUS "rtsched: using system GoogleTest")
    endif()
  endif()

  if(NOT RTSCHED_GTEST_TARGETS)
    # Fetched rather than vendored so a fresh clone builds with no manual
    # setup, and pinned to a release tag so CI is reproducible.
    message(STATUS "rtsched: fetching GoogleTest (no system install found)")
    include(FetchContent)
    FetchContent_Declare(googletest
      GIT_REPOSITORY https://github.com/google/googletest.git
      GIT_TAG v1.15.2
      GIT_SHALLOW TRUE)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    set(RTSCHED_GTEST_TARGETS gtest_main)
  endif()

  enable_testing()
  include(CTest)

  # Unit tests: fast, deterministic, safe to run on every commit.
  add_executable(rtsched_unit_tests
    tests/task_test.cpp
    tests/mpmc_queue_test.cpp
    tests/metrics_test.cpp
    tests/scheduler_test.cpp)
  target_link_libraries(rtsched_unit_tests PRIVATE rtsched ${RTSCHED_GTEST_TARGETS})

  # Stress tests: minutes of wall clock and every core on the box. Kept in a
  # separate binary and CTest label so `ctest -L unit` stays quick.
  add_executable(rtsched_stress_tests tests/scheduler_stress_test.cpp)
  target_link_libraries(rtsched_stress_tests PRIVATE rtsched ${RTSCHED_GTEST_TARGETS})

  add_test(NAME unit_tests COMMAND rtsched_unit_tests)
  set_tests_properties(unit_tests PROPERTIES LABELS unit TIMEOUT 300)

  add_test(NAME stress_tests COMMAND rtsched_stress_tests)
  set_tests_properties(stress_tests PROPERTIES LABELS stress TIMEOUT 1800 RUN_SERIAL TRUE)
endif()

# ---------------------------------------------------------------------------
# Benchmarks and examples
# ---------------------------------------------------------------------------

if(RTSCHED_BUILD_BENCHMARKS)
  # Hand-rolled harness rather than google/benchmark: the measurement here is
  # end-to-end throughput and latency percentiles across a thread pool, which
  # is not what a per-iteration microbenchmark framework measures, and it
  # keeps the dependency list at exactly one (gtest, tests only).
  add_executable(rtsched_benchmark benchmarks/scheduler_benchmark.cpp)
  target_link_libraries(rtsched_benchmark PRIVATE rtsched)
endif()

if(RTSCHED_BUILD_EXAMPLES)
  add_executable(rtsched_example examples/basic_usage.cpp)
  target_link_libraries(rtsched_example PRIVATE rtsched)
endif()

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

include(GNUInstallDirs)
install(TARGETS rtsched rtsched_options
  EXPORT rtschedTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/rtsched DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT rtschedTargets
  FILE rtschedTargets.cmake
  NAMESPACE rtsched::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/rtsched)

message(STATUS "rtsched ${PROJECT_VERSION}: build=${CMAKE_BUILD_TYPE} "
               "tests=${RTSCHED_BUILD_TESTS} benchmarks=${RTSCHED_BUILD_BENCHMARKS}")
