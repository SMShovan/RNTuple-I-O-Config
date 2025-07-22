#include "HitWireStat.hpp"
#include <ROOT/RDataFrame.hxx>
#include <iostream>
#include <filesystem>
#include <vector>
#include <string>
#include <algorithm>
#include "Wire.hpp"
#include "Hit.hpp"
#include <iomanip> // Add this at the top

namespace fs = std::filesystem;

void printFileStats() {
    std::string outputDir = "./output";
    std::vector<std::string> files;
    for (const auto& entry : fs::directory_iterator(outputDir)) {
        if (entry.path().extension() == ".root") {
            files.push_back(entry.path().string());
        }
    }
    if (files.empty()) {
        std::cout << "No ROOT files found in " << outputDir << std::endl;
        return;
    }
    
    // Now print actual data elements
    std::cout << "\nFile statistics (actual data elements):\n";
    std::cout << std::string(64, '-') << "\n";
    std::cout << std::left
              << std::setw(32) << "File"
              << std::setw(16) << "TotalHits"
              << std::setw(16) << "TotalWires"
              << std::setw(16) << "TotalROIs" << std::endl;
    std::cout << std::string(64, '-') << "\n";
    for (const auto& file : files) {
        std::size_t totalHits = 0, totalWires = 0, totalROIs = 0;
        // Hits
        try {
            ROOT::RDataFrame dfHits("hits", file);
            const auto& colNames = dfHits.GetColumnNames();
            if (std::find(colNames.begin(), colNames.end(), "HitVector") != colNames.end()) {
                auto hits = dfHits.Take<HitVector>("HitVector");
                for (const auto& hv : *hits) totalHits += hv.fChannel.size();
            } else if (std::find(colNames.begin(), colNames.end(), "HitIndividual") != colNames.end()) {
                totalHits = dfHits.Count().GetValue();
            } else if (std::find(colNames.begin(), colNames.end(), "Hits") != colNames.end()) {
                auto hits = dfHits.Take<std::vector<HitIndividual>>("Hits");
                for (const auto& hv : *hits) totalHits += hv.size();
            } else if (std::find(colNames.begin(), colNames.end(), "Channel") != colNames.end()) {
                std::string colType = dfHits.GetColumnType("Channel");
                if (colType.find("VecOps::RVec") != std::string::npos || colType.find("vector") != std::string::npos) {
                    auto channels = dfHits.Take<std::vector<unsigned int>>("Channel");
                    for (const auto& chVec : *channels)
                        totalHits += chVec.size();
                } else {
                    // Scalar: count entries
                    totalHits = dfHits.Count().GetValue();
                }
            }
        } catch (...) { totalHits = 0; }
        // Wires (unchanged)
        try {
            ROOT::RDataFrame dfWires("wires", file);
            const auto& colNames = dfWires.GetColumnNames();
            if (std::find(colNames.begin(), colNames.end(), "WireVector") != colNames.end()) {
                auto wires = dfWires.Take<WireVector>("WireVector");
                for (const auto& wv : *wires) totalWires += wv.fWire_Channel.size();
                for (const auto& wv : *wires) {
                    if (!wv.fSignalROI_nROIs.empty()) {
                        for (auto n : wv.fSignalROI_nROIs) totalROIs += n;
                    }
                }
            } else if (std::find(colNames.begin(), colNames.end(), "WireIndividual") != colNames.end()) {
                auto wires = dfWires.Take<WireIndividual>("WireIndividual");
                totalWires = wires->size();
                for (const auto& w : *wires) totalROIs += w.fSignalROI.size();
            } else if (std::find(colNames.begin(), colNames.end(), "Wires") != colNames.end()) {
                auto wires = dfWires.Take<std::vector<WireIndividual>>("Wires");
                for (const auto& wv : *wires) {
                    totalWires += wv.size();
                    for (const auto& wi : wv) totalROIs += wi.fSignalROI.size();
                }
            }
        } catch (...) { totalWires = 0; totalROIs = 0; }
        std::cout << std::left
                  << std::setw(32) << fs::path(file).filename().string()
                  << std::setw(16) << totalHits
                  << std::setw(16) << totalWires
                  << std::setw(16) << totalROIs << std::endl;
    }
    std::cout << std::string(64, '-') << "\n";
} 