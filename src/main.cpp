#include "HitWireWriters.hpp"
#include <iostream>

int main() {
    int EventCount = 1000;
    int FieldSize = 100;
    int SpilCount = 10;

    std::cout << "Generating HitWire data with Vector format..." << std::endl;
    generateAndWriteHitWireDataVector(EventCount, FieldSize, "./hitwire/HitWireVector.root");
    
    std::cout << "Generating HitWire data with Individual format..." << std::endl;
    generateAndWriteHitWireDataIndividual(EventCount, FieldSize, "./hitwire/HitWireIndividual.root");
    
    std::cout << "Generating Split HitWire data with Vector format..." << std::endl;
    generateAndWriteSplitHitAndWireDataVector(EventCount, FieldSize, "./hitwire/split_hitwire_vector.root");
    
    std::cout << "Generating Split HitWire data with Individual format..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividual(EventCount, FieldSize, "./hitwire/split_hitwire_individual.root");
    
    std::cout << "Generating Spil HitWire data with Vector format..." << std::endl;
    generateAndWriteSpilHitAndWireDataVector(EventCount, SpilCount, FieldSize, "./hitwire/hitspils/hits_spil_all_vector.root");
    
    std::cout << "Generating Spil HitWire data with Individual format..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividual(EventCount, SpilCount, FieldSize, "./hitwire/hitspils/hits_spil_all_individual.root");

    return 0;
} 

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..