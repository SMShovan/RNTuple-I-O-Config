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

    return 0;
} 

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..