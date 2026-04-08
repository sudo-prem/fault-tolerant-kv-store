cmake_minimum_required(VERSION 3.11)
project(raft_integration_tests CXX)

file(GLOB TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

foreach(test_source ${TEST_SOURCES})
    get_filename_component(test_name ${test_source} NAME_WE)

    add_executable(${test_name} ${test_source} ${absl_INCLUDE_DIRS} ${Protobuf_INCLUDE_DIRS} )

    if (TRACING)
        target_link_libraries(${test_name} PRIVATE
            rafty raft_grpc_proto tester_grpc_proto kv_grpc_proto
            gtest gtest_main
            absl::flags
            absl::flags_parse
            paho-mqtt3c # ddb mqtt client
            opentelemetry-cpp::otlp_grpc_exporter
            opentelemetry-cpp::otlp_grpc_log_record_exporter
            opentelemetry-cpp::trace
            opentelemetry-cpp::logs
            opentelemetry_contrib::spdlog_sink
        )
    else()
        target_link_libraries(${test_name} PRIVATE
            rafty raft_grpc_proto tester_grpc_proto kv_grpc_proto
            gtest gtest_main
            absl::flags
            absl::flags_parse
            paho-mqtt3c # ddb mqtt client
        )
    endif()
endforeach()