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

# --- ROOT Dictionary Generation ---
set(DICT_HEADERS ${CMAKE_SOURCE_DIR}/include/Wire.hpp)
set(LINKDEF_FILE ${CMAKE_SOURCE_DIR}/include/WireLinkDef.h)
set(DICT_OUTPUT ${CMAKE_BINARY_DIR}/WireDict.cxx)

add_custom_command(
    OUTPUT ${DICT_OUTPUT}
    COMMAND rootcling -f ${DICT_OUTPUT} -c ${DICT_HEADERS} ${LINKDEF_FILE}
    DEPENDS ${DICT_HEADERS} ${LINKDEF_FILE}
    COMMENT "Generating ROOT dictionary"
)

set(DICTIONARY_SOURCES ${DICT_OUTPUT} ${CMAKE_SOURCE_DIR}/src/Wire.cpp)
add_executable(hitwire ${SOURCES} ${DICTIONARY_SOURCES})

target_compile_options(hitwire PRIVATE ${ROOT_CFLAGS})
target_link_libraries(hitwire PRIVATE ${ROOT_LIBS})