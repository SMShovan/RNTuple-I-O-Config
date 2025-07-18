cmake_minimum_required(VERSION 3.10)

add_executable(test_split_range test_split_range.cpp ../src/Utils.cpp)
target_link_libraries(test_split_range gtest_main)
target_include_directories(test_split_range PRIVATE ../include)
add_test(NAME test_split_range COMMAND test_split_range)

add_executable(test_split_range_by_clusters test_split_range_by_clusters.cpp)
target_link_libraries(test_split_range_by_clusters gtest_main gmock_main)
target_include_directories(test_split_range_by_clusters PRIVATE ../include)
add_test(NAME test_split_range_by_clusters COMMAND test_split_range_by_clusters) 