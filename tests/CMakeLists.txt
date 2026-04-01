include(GoogleTest)

# Unit tests
file(GLOB UNIT_TEST_SOURCES unit/*.cc)
add_executable(kubemq_unit_tests ${UNIT_TEST_SOURCES})
target_link_libraries(kubemq_unit_tests
    PRIVATE kubemq_static GTest::gtest_main GTest::gmock)
target_include_directories(kubemq_unit_tests
    PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_BINARY_DIR}/generated)
gtest_discover_tests(kubemq_unit_tests)

# Integration tests (require live broker)
file(GLOB INTEGRATION_TEST_SOURCES integration/*.cc)
add_executable(kubemq_integration_tests ${INTEGRATION_TEST_SOURCES})
target_link_libraries(kubemq_integration_tests
    PRIVATE kubemq_static GTest::gtest_main)
gtest_discover_tests(kubemq_integration_tests
    PROPERTIES LABELS "integration")

# Benchmarks
if(KUBEMQ_BUILD_BENCHMARKS)
    find_package(benchmark CONFIG REQUIRED)
    add_executable(kubemq_benchmarks benchmarks/throughput_bench.cc)
    target_link_libraries(kubemq_benchmarks
        PRIVATE kubemq_static benchmark::benchmark benchmark::benchmark_main)
endif()
