cmake_minimum_required(VERSION 3.10)
project(HitWireProject)

set(CMAKE_CXX_STANDARD 17)

# Use root-config to get the flags
execute_process(
    COMMAND root-config --cflags
    OUTPUT_VARIABLE ROOT_CFLAGS
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND root-config --libs
    OUTPUT_VARIABLE ROOT_LIBS
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

separate_arguments(ROOT_CFLAGS)
separate_arguments(ROOT_LIBS)

include_directories(${CMAKE_SOURCE_DIR}/include)

file(GLOB SOURCES "src/*.cpp")

add_executable(hitwire ${SOURCES})

target_compile_options(hitwire PRIVATE ${ROOT_CFLAGS})
target_link_libraries(hitwire PRIVATE ${ROOT_LIBS})