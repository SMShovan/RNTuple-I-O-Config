#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <TFile.h>
#include <iostream>
#include <map>
#include <string>

using namespace ROOT::Experimental;

struct DataCounts {
    int hitEntries = 0;
    int wireEntries = 0;
    std::string fileName;
};

DataCounts countEntries(const std::string& fileName, const std::string& hitNtupleName, const std::string& wireNtupleName) {
    DataCounts counts;
    counts.fileName = fileName;
    
    try {
        TFile file(fileName.c_str(), "READ");
        if (file.IsZombie()) {
            std::cout << "Error: Could not open file " << fileName << std::endl;
            return counts;
        }
        
        // Count hit entries
        auto hitReader = RNTupleReader::Open(hitNtupleName, fileName);
        if (hitReader) {
            counts.hitEntries = hitReader->GetNEntries();
        }
        
        // Count wire entries
        auto wireReader = RNTupleReader::Open(wireNtupleName, fileName);
        if (wireReader) {
            counts.wireEntries = wireReader->GetNEntries();
        }
        
        file.Close();
    } catch (const std::exception& e) {
        std::cout << "Error reading " << fileName << ": " << e.what() << std::endl;
    }
    
    return counts;
}

int main() {
    std::cout << "=== Data Count Verification for All 6 Experiments ===" << std::endl;
    std::cout << "Parameters: 1000 events, 100 hits per event, 10 spils" << std::endl;
    std::cout << "Expected total data points: 1000 * 100 = 100,000" << std::endl;
    std::cout << "Expected spil data points: 1000 * 10 = 10,000 spil entries" << std::endl;
    std::cout << std::endl;
    
    std::map<std::string, DataCounts> experiments;
    
    // Define all experiments with their file names and ntuple names
    experiments["HitWire Vector"] = countEntries("./hitwire/HitWireVector.root", "HitWireVector", "WireVector");
    experiments["HitWire Individual"] = countEntries("./hitwire/HitWireIndividual.root", "HitWireIndividual", "WireIndividual");
    experiments["Split HitWire Vector"] = countEntries("./hitwire/split_hitwire_vector.root", "HitVector", "WireVector");
    experiments["Split HitWire Individual"] = countEntries("./hitwire/split_hitwire_individual.root", "HitIndividual", "WireIndividual");
    experiments["Spil HitWire Vector"] = countEntries("./hitwire/hitspils/hits_spil_all_vector.root", "HitSpilVector", "WireSpilVector");
    experiments["Spil HitWire Individual"] = countEntries("./hitwire/hitspils/hits_spil_all_individual.root", "HitSpilIndividual", "WireSpilIndividual");
    
    // Expected values
    const int expectedEvents = 1000;
    const int expectedHitsPerEvent = 100;
    const int expectedTotalHits = expectedEvents * expectedHitsPerEvent;
    const int expectedSpilEvents = expectedEvents * 10; // 10 spils per event
    const int expectedSpilHitsPerEvent = expectedHitsPerEvent / 10; // hits divided across spils
    const int expectedTotalSpilHits = expectedSpilEvents * expectedSpilHitsPerEvent;
    
    std::cout << "=== Results ===" << std::endl;
    std::cout << std::left << std::setw(25) << "Experiment" 
              << std::setw(15) << "Hit Entries" 
              << std::setw(15) << "Wire Entries" 
              << std::setw(10) << "Status" << std::endl;
    std::cout << std::string(65, '-') << std::endl;
    
    bool allMatch = true;
    
    for (const auto& [name, counts] : experiments) {
        bool hitMatch = false;
        bool wireMatch = false;
        std::string status = "OK";
        
        // Check if this is a spil experiment
        if (name.find("Spil") != std::string::npos) {
            hitMatch = (counts.hitEntries == expectedTotalSpilHits);
            wireMatch = (counts.wireEntries == expectedTotalSpilHits);
        } else {
            hitMatch = (counts.hitEntries == expectedTotalHits);
            wireMatch = (counts.wireEntries == expectedTotalHits);
        }
        
        if (!hitMatch || !wireMatch) {
            status = "MISMATCH";
            allMatch = false;
        }
        
        std::cout << std::left << std::setw(25) << name
                  << std::setw(15) << counts.hitEntries
                  << std::setw(15) << counts.wireEntries
                  << std::setw(10) << status << std::endl;
    }
    
    std::cout << std::endl;
    std::cout << "=== Summary ===" << std::endl;
    if (allMatch) {
        std::cout << "✓ ALL EXPERIMENTS STORE EXACTLY THE SAME NUMBER OF DATA!" << std::endl;
    } else {
        std::cout << "✗ SOME EXPERIMENTS HAVE DIFFERENT DATA COUNTS!" << std::endl;
    }
    
    std::cout << std::endl;
    std::cout << "=== Expected Values ===" << std::endl;
    std::cout << "Regular experiments (Vector/Individual/Split): " << expectedTotalHits << " entries each" << std::endl;
    std::cout << "Spil experiments: " << expectedTotalSpilHits << " entries each" << std::endl;
    std::cout << "Note: Spil experiments divide hits across multiple spil files" << std::endl;
    
    return allMatch ? 0 : 1;
} 