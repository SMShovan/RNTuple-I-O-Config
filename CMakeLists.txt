cmake_minimum_required(VERSION 3.10)
project(HitWireProject)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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
execute_process(
    COMMAND root-config --incdir
    OUTPUT_VARIABLE ROOT_INCDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

separate_arguments(ROOT_CFLAGS)
separate_arguments(ROOT_LIBS)

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${ROOT_INCDIR})

file(GLOB SOURCES "src/*.cpp")

# --- ROOT Dictionary Generation ---
set(DICT_HEADERS ${CMAKE_SOURCE_DIR}/include/Wire.hpp ${CMAKE_SOURCE_DIR}/include/Hit.hpp)
set(LINKDEF_FILE ${CMAKE_SOURCE_DIR}/include/WireLinkDef.h)
set(DICT_OUTPUT ${CMAKE_BINARY_DIR}/WireDict.cxx)

add_custom_command(
    OUTPUT ${DICT_OUTPUT}
    COMMAND rootcling -f ${DICT_OUTPUT} -c ${DICT_HEADERS} ${LINKDEF_FILE}
    DEPENDS ${DICT_HEADERS} ${LINKDEF_FILE}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

set(DICTIONARY_SOURCES ${DICT_OUTPUT})
add_executable(hitwire
    src/main.cpp
    src/HitWireWriters.cpp
    src/HitWireGenerators.cpp
    src/HitWireReaders.cpp
    src/Utils.cpp
    src/HitWireWriterHelpers.cpp
    src/HitWireStat.cpp
)

target_compile_options(hitwire PRIVATE ${ROOT_CFLAGS})
target_link_libraries(hitwire PRIVATE ${ROOT_LIBS} WireDict)

# Add this section to build the dictionary as a shared library
add_library(WireDict SHARED ${DICT_OUTPUT})
target_include_directories(WireDict PRIVATE ${ROOT_INCDIR})
target_link_libraries(WireDict PRIVATE ${ROOT_LIBS})

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/heads/main.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()
add_subdirectory(tests)