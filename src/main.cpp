#include "HitWireWriters.hpp"
#include <iostream>

int main() {
    int NumEvents = 1000;
    int HitsPerEvent = 100;
    int NumSpils = 10;

    std::cout << "Generating HitWire data with Vector format..." << std::endl;
    generateAndWriteHitWireDataVector(NumEvents, HitsPerEvent, "./hitwire/HitWireVector.root");
    
    std::cout << "Generating HitWire data with Individual format..." << std::endl;
    generateAndWriteHitWireDataIndividual(NumEvents, HitsPerEvent, "./hitwire/HitWireIndividual.root");
    
    std::cout << "Generating Split HitWire data with Vector format..." << std::endl;
    generateAndWriteSplitHitAndWireDataVector(NumEvents, HitsPerEvent, "./hitwire/split_hitwire_vector.root");
    
    std::cout << "Generating Split HitWire data with Individual format..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividual(NumEvents, HitsPerEvent, "./hitwire/split_hitwire_individual.root");
    
    std::cout << "Generating Spil HitWire data with Vector format..." << std::endl;
    generateAndWriteSpilHitAndWireDataVector(NumEvents, NumSpils, HitsPerEvent, "./hitwire/hitspils/hits_spil_all_vector.root");
    
    std::cout << "Generating Spil HitWire data with Individual format..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividual(NumEvents, NumSpils, HitsPerEvent, "./hitwire/hitspils/hits_spil_all_individual.root");

    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Generating HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteHitWireDataVectorDict(NumEvents, HitsPerEvent, "./hitwire/HitWireVectorDict.root");
    std::cout << "Generating HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteHitWireDataIndividualDict(NumEvents, HitsPerEvent, "./hitwire/HitWireIndividualDict.root");
    std::cout << "Generating Split HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataVectorDict(NumEvents, HitsPerEvent, "./hitwire/split_hitwire_vector_dict.root");
    std::cout << "Generating Split HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividualDict(NumEvents, HitsPerEvent, "./hitwire/split_hitwire_individual_dict.root");
    std::cout << "Generating Spil HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataVectorDict(NumEvents, NumSpils, HitsPerEvent, "./hitwire/hitspils/hits_spil_all_vector_dict.root");
    std::cout << "Generating Spil HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividualDict(NumEvents, NumSpils, HitsPerEvent, "./hitwire/hitspils/hits_spil_all_individual_dict.root");

    return 0;
} 

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..