file(GLOB_RECURSE BURNIN_SOURCES *.cc)
add_executable(kubemq_burnin ${BURNIN_SOURCES})
target_link_libraries(kubemq_burnin
    PRIVATE kubemq_static httplib::httplib nlohmann_json::nlohmann_json yaml-cpp)
target_include_directories(kubemq_burnin
    PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
