#include "Utils.hpp"
#include <random>
#include <vector>
#include <utility>
#include <cstddef>

namespace Utils {

std::vector<unsigned int> generateSeeds(int numThreads) {
    // Use a constant seed for reproducibility
    std::mt19937 gen(42); // 42 is the chosen constant seed
    std::vector<unsigned int> seeds;
    for (int i = 0; i < numThreads; ++i) {
        seeds.push_back(gen());
    }
    return seeds;
}

std::vector<std::pair<std::size_t, std::size_t>> split_range(std::size_t begin, std::size_t end, int nChunks) {
    std::vector<std::pair<std::size_t, std::size_t>> chunks;
    std::size_t total = end - begin;
    std::size_t chunkSize = total / nChunks;
    std::size_t remainder = total % nChunks;
    std::size_t current = begin;
    for (int i = 0; i < nChunks; ++i) {
        std::size_t next = current + chunkSize + (i < remainder ? 1 : 0);
        chunks.emplace_back(current, next);
        current = next;
    }
    return chunks;
}

} // namespace Utils 