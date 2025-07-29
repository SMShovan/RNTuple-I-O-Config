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
#include <TFile.h> // For checking keys

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

        auto rootFile = TFile::Open(file.c_str(), "READ");
        if (!rootFile || rootFile->IsZombie()) {
            continue; // Skip invalid files
        }

        // Hits
        if (rootFile->Get("hits")) {
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
        }

        // Wires
        if (rootFile->Get("wires")) {
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
        }

        rootFile->Close();

        std::cout << std::left
                  << std::setw(32) << fs::path(file).filename().string()
                  << std::setw(16) << totalHits
                  << std::setw(16) << totalWires
                  << std::setw(16) << totalROIs << std::endl;
    }
    std::cout << std::string(64, '-') << "\n";
} 

void printEntryCounts() {
    std::string outputDir = "./output";
    std::vector<std::pair<std::string, std::string>> writerFiles = {
        {"Hit/Wire Vector", "vector.root"},
        {"Hit/Wire Individual", "individual.root"},
        {"VertiSplit-Vector", "VertiSplit_vector.root"},
        {"VertiSplit-Individual", "VertiSplit_individual.root"},
        {"HoriSpill-Vector", "HoriSpill_vector.root"},
        {"HoriSpill-Individual", "HoriSpill_individual.root"},
        {"Vector-Dict", "vector_dict.root"},
        {"Individual-Dict", "individual_dict.root"},
        {"VertiSplit-Vector-Dict", "VertiSplit_vector_dict.root"},
        {"VertiSplit-Individual-Dict", "VertiSplit_individual_dict.root"},
        {"HoriSpill-Vector-Dict", "HoriSpill_vector_dict.root"},
        {"HoriSpill-Individual-Dict", "HoriSpill_individual_dict.root"},
        {"Vector-of-Individuals", "vector_of_individuals.root"}
    };

    std::cout << std::left
              << std::setw(32) << "Writer"
              << std::setw(16) << "Hits Entries"
              << std::setw(16) << "Wires Entries" << std::endl;
    std::cout << std::string(64, '-') << std::endl;

    for (const auto& [label, fileName] : writerFiles) {
        std::string filePath = outputDir + "/" + fileName;
        long long hitsCount = 0;
        long long wiresCount = 0;
        if (std::filesystem::exists(filePath)) {
            ROOT::RDataFrame df_hits("hits", filePath);
            hitsCount = *df_hits.Count();
            ROOT::RDataFrame df_wires("wires", filePath);
            wiresCount = *df_wires.Count();
        }
        std::cout << std::left
                  << std::setw(32) << label
                  << std::setw(16) << hitsCount
                  << std::setw(16) << wiresCount << std::endl;
    }
    std::cout << std::string(64, '-') << std::endl;
} 