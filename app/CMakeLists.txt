cmake_minimum_required(VERSION 3.11)
project(raft_apps CXX)

# Find all test source files
file(GLOB APP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# Include the header files directory (already done in the root CMakeLists.txt)
#include_directories(${CMAKE_SOURCE_DIR}/inc)
#include_directories(${CMAKE_SOURCE_DIR}/generated)

# For each test source file, create a test executable and link with the librafty
foreach(app_source ${APP_SOURCES})
    # Get the test name from the file name
    get_filename_component(app_name ${app_source} NAME_WE)

    # Create the test executable
    add_executable(${app_name} ${app_source})

    # Link the test executable with the librafty and the generated protobuf library
    if (TRACING)
        target_link_libraries(${app_name} PRIVATE
            rafty raft_grpc_proto tester_grpc_proto kv_grpc_proto
            spdlog::spdlog $<$<BOOL:${MINGW}>:ws2_32>
            absl::check
            absl::flags
            absl::flags_parse
            absl::log
            ${_REFLECTION}
            ${_GRPC_GRPCPP}
            ${_PROTOBUF_LIBPROTOBUF}
            gtest gtest_main
            paho-mqtt3c # ddb mqtt client
            opentelemetry-cpp::otlp_grpc_exporter
            opentelemetry-cpp::otlp_grpc_log_record_exporter
            opentelemetry-cpp::trace
            opentelemetry-cpp::logs
            opentelemetry_contrib::spdlog_sink
        )
    else()
        target_link_libraries(${app_name} PRIVATE
            rafty raft_grpc_proto tester_grpc_proto kv_grpc_proto
            spdlog::spdlog $<$<BOOL:${MINGW}>:ws2_32>
            absl::check
            absl::flags
            absl::flags_parse
            absl::log
            ${_REFLECTION}
            ${_GRPC_GRPCPP}
            ${_PROTOBUF_LIBPROTOBUF}
            gtest gtest_main
            paho-mqtt3c # ddb mqtt client
        )
    endif()
endforeach()