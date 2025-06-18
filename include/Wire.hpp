#pragma once
#include <vector>

class WireSoA {
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

// AoS version for a single wire
struct WireAoS {
    long long EventID;
    unsigned int fWire_Channel;
    int fWire_View;
    unsigned int fSignalROI_nROIs;
    std::size_t fSignalROI_offsets;
    float fSignalROI_data;
};

// AoS generator declaration
WireAoS generateRandomWireAoS(long long eventID); 