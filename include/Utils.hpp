#ifndef UTILS_HPP
#define UTILS_HPP

#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace ROOT { class RNTupleReader; }

namespace Utils {

/**
 * @brief Generates unique random seeds for multiple threads.
 *
 * Uses a fixed seed for reproducibility while providing distinct seeds per thread.
 * Avoids expensive random_device calls in each thread.
 *
 * @param numThreads Number of seeds to generate (must be > 0).
 * @return Vector of unsigned int seeds, one per thread.
 */
std::vector<unsigned int> generateSeeds(int numThreads);

/**
 * @brief Splits the entry range of an RNTuple into approximately equal chunks based on its clusters.
 *
 * This function divides the total entries across the NTuple's clusters into nChunks,
 * aiming for load-balanced ranges. It ensures chunks align with cluster boundaries for
 * efficient parallel processing. If there are fewer clusters than nChunks, some chunks
 * may be empty or combined.
 *
 * @param reader Pointer to an open RNTupleReader for accessing the NTuple descriptor.
 * @param nChunks Number of chunks to split into (should be > 0; if 0, returns empty).
 * @return Vector of pairs representing [start, end) entry ranges for each chunk.
 *
 * @note Assumes the reader is valid and open. For empty NTuples (0 clusters), returns an empty vector.
 * @warning If nChunks > number of clusters, some returned chunks may be empty.
 * @example
 * auto chunks = Utils::split_range_by_clusters(reader.get(), 4);
 * // chunks might be {{0,250}, {250,500}, {500,750}, {750,1000}}
 */
std::vector<std::pair<std::size_t, std::size_t>> split_range_by_clusters(ROOT::RNTupleReader& reader, int nChunks);

// Deterministic seeding helpers for per-logical-entry RNG

// Global base seed constant used to derive per-entry seeds deterministically.
constexpr std::uint64_t kBaseSeed = 0xC0FFEE1234ABCDEFULL;

// SplitMix64 hash mixer (64-bit), stable across platforms/compilers.
inline std::uint64_t splitmix64(std::uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

template <typename T>
inline void hash_combine64(std::uint64_t& h, const T& v) {
    std::uint64_t x = static_cast<std::uint64_t>(v);
    h = splitmix64(h ^ x);
}

template <typename... Ts>
inline std::uint32_t make_seed(std::uint64_t base_seed, Ts... parts) {
    std::uint64_t h = base_seed;
    (hash_combine64(h, parts), ...);
    return static_cast<std::uint32_t>(h ^ (h >> 32));
}

} // namespace Utils

#endif // UTILS_HPP 