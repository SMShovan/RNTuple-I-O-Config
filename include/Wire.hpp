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

    std::vector<std::size_t>& getSignalROI_offsets() { return fSignalROI_offsets; }
    std::vector<float>& getSignalROI_data() { return fSignalROI_data; }
}; 