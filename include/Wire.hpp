#pragma once

#include <vector>
#include <random>
#include <TObject.h>




class WireVector  {
public:
    long long EventID;
    std::vector<unsigned int> fWire_Channel;
    std::vector<int> fWire_View;
    std::vector<unsigned int> fSignalROI_nROIs;
    std::vector<std::size_t> fSignalROI_offsets;
    std::vector<float> fSignalROI_data;

    WireVector() = default;
    
    std::vector<unsigned int>& getWire_Channel() { return fWire_Channel; }
    std::vector<int>& getWire_View() { return fWire_View; }
    std::vector<unsigned int>& getSignalROI_nROIs() { return fSignalROI_nROIs; }
    std::vector<std::size_t>& getSignalROI_offsets() { return fSignalROI_offsets; }
    std::vector<float>& getSignalROI_data() { return fSignalROI_data; }
    ClassDef(WireVector, 3)
};

// Forward-declare the struct and its generator function
struct WireIndividual;
WireIndividual generateRandomWireIndividual(long long eventID, int nROIs, std::mt19937& rng);

struct RegionOfInterest {
    std::size_t offset;
    std::vector<float> data;

    RegionOfInterest() = default;
    
    ClassDef(RegionOfInterest, 3)
};
using RegionsOfInterest_t = std::vector<RegionOfInterest>;

struct WireIndividual {
    long long EventID;
    unsigned int fWire_Channel;
    int fWire_View;
    RegionsOfInterest_t fSignalROI;

    WireIndividual() = default;
    
    WireIndividual(const RegionsOfInterest_t& roi, unsigned int channel, int view)
        : fWire_Channel(channel), fWire_View(view), fSignalROI(roi) {}

    RegionsOfInterest_t& getSignalROI() { return fSignalROI; }
    ClassDef(WireIndividual, 3)
}; 