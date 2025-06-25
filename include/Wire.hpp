#pragma once

#include <vector>
#include <random>

class WireVector {
public:
    long long EventID;
    std::vector<unsigned int> fWire_Channel;
    std::vector<int> fWire_View;
    std::vector<unsigned int> fSignalROI_nROIs;
    std::vector<std::size_t> fSignalROI_offsets;
    std::vector<float> fSignalROI_data;

    // Pre-allocate all vectors to avoid dynamic resizing
    void reserve(size_t size) {
        fWire_Channel.reserve(size);
        fWire_View.reserve(size);
        fSignalROI_nROIs.reserve(size);
        // For ROI data, we need to estimate the total size
        // Each wire has 1-3 ROIs, each ROI has 1-10 data points
        // So we estimate: size * 3 (max ROIs) * 10 (max data per ROI) = size * 30
        fSignalROI_offsets.reserve(size * 3);  // Max 3 ROIs per wire
        fSignalROI_data.reserve(size * 30);    // Max 30 data points per wire
    }

    std::vector<unsigned int>& getWire_Channel() { return fWire_Channel; }
    std::vector<int>& getWire_View() { return fWire_View; }
    std::vector<unsigned int>& getSignalROI_nROIs() { return fSignalROI_nROIs; }
    std::vector<std::size_t>& getSignalROI_offsets() { return fSignalROI_offsets; }
    std::vector<float>& getSignalROI_data() { return fSignalROI_data; }
};

// Forward-declare the struct and its generator function to handle the friend relationship
struct WireIndividual;
WireIndividual generateRandomWireIndividual(long long eventID, int nROIs, std::mt19937& rng);

// Individual version for a single wire
struct WireIndividual {
    // The generator needs to be a friend to access private members.
    friend WireIndividual generateRandomWireIndividual(long long eventID, int nROIs, std::mt19937& rng);

    long long EventID;
    unsigned int fWire_Channel;
    int fWire_View;
    std::vector<std::size_t> fSignalROI_offsets;
    std::vector<float> fSignalROI_data;

    // Pre-allocate vectors for individual wire
    void reserve(size_t size) {
        fSignalROI_offsets.reserve(size);
        fSignalROI_data.reserve(size * 10);  // Estimate 10 data points per ROI
    }

    std::vector<std::size_t>& getSignalROI_offsets() { return fSignalROI_offsets; }
    std::vector<float>& getSignalROI_data() { return fSignalROI_data; }
}; 