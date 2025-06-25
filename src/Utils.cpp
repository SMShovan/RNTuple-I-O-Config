#include "Utils.hpp"
#include <random>

namespace Utils {

std::vector<unsigned int> generateSeeds(int numThreads) {
    std::random_device rd;
    std::vector<unsigned int> seeds;
    for (int i = 0; i < numThreads; ++i) {
        seeds.push_back(rd());
    }
    return seeds;
}

} // namespace Utils 