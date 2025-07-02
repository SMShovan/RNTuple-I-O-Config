#include "HitWireWriters.hpp"
#include <iostream>

int main() {
    int numEvents = 100;
    int hitsPerEvent = 10;
    int wiresPerEvent = 10;
    int numSpils = 5;

    std::cout << "Generating HitWire data with Vector format..." << std::endl;
    generateAndWriteHitWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/HitWireVector.root");
    
    std::cout << "Generating HitWire data with Individual format..." << std::endl;
    generateAndWriteHitWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/HitWireIndividual.root");
    
    std::cout << "Generating Split HitWire data with Vector format..." << std::endl;
    generateAndWriteSplitHitAndWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/split_hitwire_vector.root");
    
    std::cout << "Generating Split HitWire data with Individual format..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/split_hitwire_individual.root");
    
    std::cout << "Generating Spil HitWire data with Vector format..." << std::endl;
    generateAndWriteSpilHitAndWireDataVector(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./hitwire/hitspils/hits_spil_all_vector.root");
    
    std::cout << "Generating Spil HitWire data with Individual format..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividual(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./hitwire/hitspils/hits_spil_all_individual.root");

    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Generating HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteHitWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/HitWireVectorDict.root");
    std::cout << "Generating HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteHitWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/HitWireIndividualDict.root");
    std::cout << "Generating Split HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/split_hitwire_vector_dict.root");
    std::cout << "Generating Split HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/split_hitwire_individual_dict.root");
    std::cout << "Generating Spil HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataVectorDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./hitwire/hitspils/hits_spil_all_vector_dict.root");
    std::cout << "Generating Spil HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividualDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./hitwire/hitspils/hits_spil_all_individual_dict.root");
    std::cout << "Generating HitWire data with Vector of Individuals format..." << std::endl;
    generateAndWriteHitWireDataVectorOfIndividuals(numEvents, hitsPerEvent, wiresPerEvent, "./hitwire/HitWireVectorOfIndividuals.root");

    return 0;
} 

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..