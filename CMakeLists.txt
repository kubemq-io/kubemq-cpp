cmake_minimum_required(VERSION 3.16)
project(kubemq-cpp VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)

# Export compile_commands.json for clang-tidy
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Options
option(KUBEMQ_BUILD_TESTS "Build tests" ON)
option(KUBEMQ_BUILD_EXAMPLES "Build examples" ON)
option(KUBEMQ_BUILD_BURNIN "Build burn-in application" ON)
option(KUBEMQ_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(KUBEMQ_ENABLE_OTEL "Enable OpenTelemetry integration" OFF)
option(KUBEMQ_BUILD_SHARED "Build shared library" ON)
option(KUBEMQ_BUILD_STATIC "Build static library" ON)

# Dependencies
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)  # Used by core library (channel management JSON parsing)

if(KUBEMQ_ENABLE_OTEL)
    find_package(opentelemetry-cpp CONFIG REQUIRED)
endif()

if(KUBEMQ_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
endif()

if(KUBEMQ_BUILD_BURNIN)
    find_package(httplib CONFIG REQUIRED)
endif()

# Proto codegen
set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/kubemq.proto)
set(PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_OUT})

add_custom_command(
    OUTPUT
        ${PROTO_OUT}/kubemq.pb.h ${PROTO_OUT}/kubemq.pb.cc
        ${PROTO_OUT}/kubemq.grpc.pb.h ${PROTO_OUT}/kubemq.grpc.pb.cc
        ${PROTO_OUT}/kubemq_mock.grpc.pb.h
    COMMAND protobuf::protoc
        --grpc_out=generate_mock_code=true:${PROTO_OUT}
        --cpp_out=lite:${PROTO_OUT}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        -I ${CMAKE_CURRENT_SOURCE_DIR}/src/proto
        kubemq.proto
    # Fix gRPC service path: the proto uses package kubemq_pb to avoid C++
    # namespace collision with the SDK's own kubemq:: namespace, but the
    # KubeMQ broker registers the service as kubemq.kubemq. Patch the
    # generated method path strings in the .cc file only (the .h file
    # has no service path strings and must not be modified).
    COMMAND ${CMAKE_COMMAND} -DFILE=${PROTO_OUT}/kubemq.grpc.pb.cc
        -DOLD_STR=/kubemq_pb.kubemq/ -DNEW_STR=/kubemq.kubemq/
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patch_string.cmake
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf/gRPC stubs (LITE_RUNTIME + mocks) with service path fix"
)

# Collect sources
file(GLOB_RECURSE KUBEMQ_SOURCES
    src/*.cc
)

# Proto generated sources
set(PROTO_SOURCES
    ${PROTO_OUT}/kubemq.pb.cc
    ${PROTO_OUT}/kubemq.grpc.pb.cc
)

# Shared library
if(KUBEMQ_BUILD_SHARED)
    add_library(kubemq SHARED ${KUBEMQ_SOURCES} ${PROTO_SOURCES})
    target_compile_definitions(kubemq PRIVATE KUBEMQ_EXPORTS)
    target_include_directories(kubemq
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${PROTO_OUT}
    )
    target_link_libraries(kubemq
        PUBLIC gRPC::grpc++ protobuf::libprotobuf-lite
        PRIVATE nlohmann_json::nlohmann_json
    )
    set_target_properties(kubemq PROPERTIES
        SOVERSION ${PROJECT_VERSION_MAJOR}
        VERSION ${PROJECT_VERSION}
    )
endif()

# Static library
if(KUBEMQ_BUILD_STATIC)
    add_library(kubemq_static STATIC ${KUBEMQ_SOURCES} ${PROTO_SOURCES})
    target_compile_definitions(kubemq_static PUBLIC KUBEMQ_STATIC)
    target_include_directories(kubemq_static
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${PROTO_OUT}
    )
    target_link_libraries(kubemq_static
        PUBLIC gRPC::grpc++ protobuf::libprotobuf-lite
        PRIVATE nlohmann_json::nlohmann_json
    )
endif()

# Tests
if(KUBEMQ_BUILD_TESTS)
    add_subdirectory(tests)
endif()

# Examples
if(KUBEMQ_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Burn-in
if(KUBEMQ_BUILD_BURNIN)
    add_subdirectory(burnin)
endif()

# Install
if(KUBEMQ_BUILD_SHARED)
    install(TARGETS kubemq
        EXPORT KubeMQCppTargets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()

if(KUBEMQ_BUILD_STATIC)
    install(TARGETS kubemq_static
        EXPORT KubeMQCppTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT KubeMQCppTargets
    FILE KubeMQCppTargets.cmake
    NAMESPACE KubeMQ::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/KubeMQCpp)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    KubeMQCppConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

configure_package_config_file(
    cmake/KubeMQCppConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/KubeMQCppConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/KubeMQCpp)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/KubeMQCppConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/KubeMQCppConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/KubeMQCpp)
