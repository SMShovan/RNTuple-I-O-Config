#ifndef UTILS_HPP
#define UTILS_HPP

#include <vector>
#include <random>

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

} // namespace Utils

#endif // UTILS_HPP 