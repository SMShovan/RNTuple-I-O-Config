#ifndef UTILS_HPP
#define UTILS_HPP

#include <vector>
#include <utility>
#include <cstddef>

namespace Utils {

/**
 * @brief Generate random seeds for multiple threads
 * @param numThreads Number of threads that need seeds
 * @return Vector of unique random seeds
 * 
 * This function creates high-quality random seeds for each thread
 * to ensure good randomness while avoiding expensive random_device
 * calls in each thread.
 */
std::vector<unsigned int> generateSeeds(int numThreads);

std::vector<std::pair<std::size_t, std::size_t>> split_range(std::size_t begin, std::size_t end, int nChunks);

} // namespace Utils

#endif // UTILS_HPP 