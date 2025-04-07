cmake_minimum_required(VERSION 3.10)

project(Mini2)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Debug flags
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Wextra -Wpedantic")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -Wall -Wextra -Wpedantic")

# Enable gRPC debug logging in Debug mode
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_definitions(
    GRPC_VERBOSITY=DEBUG
    GRPC_TRACE=all
  )
endif()

# Add this line to help find abseil
set(ABSL_PROPAGATE_CXX_STD ON)

# Add after project(Mini2)
set(ABSL_USE_EXTERNAL_GOOGLETEST OFF)
set(BUILD_TESTING OFF)

# Add this to ensure proper linking
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Add these lines
add_compile_options(-fclang-abi-compat=17)  # Fixes Clang 19 ↔ Abseil symbol issues
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-as-needed")

# Find OpenSSL explicitly
find_package(OpenSSL REQUIRED)

# Find required packages
find_package(Threads REQUIRED)
find_package(absl CONFIG REQUIRED)
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Find gRPC executables
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
find_program(PROTOC protoc REQUIRED)

# Generate protobuf and gRPC code
get_filename_component(mini2_proto "proto/mini2.proto" ABSOLUTE)
get_filename_component(mini2_proto_path "${mini2_proto}" PATH)

# Generate gRPC files 
set(mini2_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/proto/mini2.pb.cc")
set(mini2_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/proto/mini2.pb.h")
set(mini2_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/proto/mini2.grpc.pb.cc")
set(mini2_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/proto/mini2.grpc.pb.h")

# Add protobuf and gRPC targets
add_custom_command(
    OUTPUT 
        "${mini2_proto_srcs}"
        "${mini2_proto_hdrs}"
        "${mini2_grpc_srcs}"
        "${mini2_grpc_hdrs}"
    COMMAND ${PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/proto"
         --cpp_out "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/proto"
         -I "${CMAKE_CURRENT_SOURCE_DIR}/proto"
         --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
         "${CMAKE_CURRENT_SOURCE_DIR}/proto/mini2.proto"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/proto/mini2.proto"
)

# Add a custom target for the generated files
add_custom_target(generate_proto_files
    DEPENDS
        "${mini2_proto_srcs}"
        "${mini2_proto_hdrs}"
        "${mini2_grpc_srcs}"
        "${mini2_grpc_hdrs}"
)

# Server source files
set(SERVER_SRC
    cpp/src/server.cpp
    cpp/src/SpatialAnalysis.cpp
    ${mini2_proto_srcs}
    ${mini2_grpc_srcs}
)

add_executable(server ${SERVER_SRC})
add_dependencies(server generate_proto_files)

# Add debug symbols even in Release mode for better crash reports
target_compile_options(server PRIVATE 
  $<$<CONFIG:Debug>:-g3 -ggdb3>
  $<$<CONFIG:RelWithDebInfo>:-g -O2>
)

# Link libraries
target_link_libraries(server PRIVATE
    gRPC::grpc++
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    OpenSSL::SSL
    OpenSSL::Crypto
    absl::flags
    absl::flags_parse
    absl::flags_reflection
    absl::base
    absl::log
    absl::log_initialize
    absl::log_entry
    absl::log_globals
    absl::log_severity
    absl::check
    absl::log_internal_check_op
    absl::log_internal_conditions
    absl::log_internal_format
    absl::log_internal_globals
    absl::log_internal_log_sink_set
    absl::log_internal_message
    absl::log_internal_nullguard
    absl::log_internal_proto
    absl::log_sink
    absl::log_sink_registry
    absl::cord
    absl::cordz_info
    absl::cord_internal
    absl::strings
    absl::strings_internal
    absl::symbolize
    absl::stacktrace
    absl::synchronization
    absl::time
    absl::time_zone
    absl::status
    absl::statusor
    Threads::Threads
)

# Make sure include_directories includes the cpp/include directory
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/include
    ${Protobuf_INCLUDE_DIRS}
    ${gRPC_INCLUDE_DIRS}
    ${ABSL_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
)

# Add address sanitizer in Debug mode for memory error detection
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_options(server PRIVATE -fsanitize=address -fno-omit-frame-pointer)
  target_link_options(server PRIVATE -fsanitize=address)
endif()

