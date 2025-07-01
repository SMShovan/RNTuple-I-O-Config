#include "Utils.hpp"
#include <random>

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

} // namespace Utils 