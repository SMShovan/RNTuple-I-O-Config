#include <gtest/gtest.h>
#include <vector>
#include <utility>
#include <cstddef>
#include "Utils.hpp"

TEST(SplitRangeTest, EvenSplit) {
    auto chunks = Utils::split_range(0, 8, 4);
    ASSERT_EQ(chunks.size(), 4);
    EXPECT_EQ(chunks[0], std::make_pair(0ul, 2ul));
    EXPECT_EQ(chunks[1], std::make_pair(2ul, 4ul));
    EXPECT_EQ(chunks[2], std::make_pair(4ul, 6ul));
    EXPECT_EQ(chunks[3], std::make_pair(6ul, 8ul));
}

TEST(SplitRangeTest, UnevenSplit) {
    auto chunks = Utils::split_range(0, 10, 3);
    ASSERT_EQ(chunks.size(), 3);
    EXPECT_EQ(chunks[0], std::make_pair(0ul, 4ul));
    EXPECT_EQ(chunks[1], std::make_pair(4ul, 7ul));
    EXPECT_EQ(chunks[2], std::make_pair(7ul, 10ul));
}

TEST(SplitRangeTest, SingleChunk) {
    auto chunks = Utils::split_range(0, 5, 1);
    ASSERT_EQ(chunks.size(), 1);
    EXPECT_EQ(chunks[0], std::make_pair(0ul, 5ul));
}

TEST(SplitRangeTest, ZeroRange) {
    auto chunks = Utils::split_range(0, 0, 3);
    ASSERT_EQ(chunks.size(), 3);
    for (const auto& chunk : chunks) {
        EXPECT_EQ(chunk.first, 0ul);
        EXPECT_EQ(chunk.second, 0ul);
    }
} 