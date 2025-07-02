#include "HitWireWriters.hpp"
#include <iostream>

int main() {
    int numEvents = 1000;
    int hitsPerEvent = 100;
    int wiresPerEvent = 100;
    int numSpils = 10;

    std::cout << "Generating HitWire data with Vector format..." << std::endl;
    generateAndWriteHitWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, "./output/vector.root");
    
    std::cout << "Generating HitWire data with Individual format..." << std::endl;
    generateAndWriteHitWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, "./output/individual.root");
    
    std::cout << "Generating Split HitWire data with Vector format..." << std::endl;
    generateAndWriteSplitHitAndWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_vector.root");
    
    std::cout << "Generating Split HitWire data with Individual format..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_individual.root");
    
    std::cout << "Generating Spil HitWire data with Vector format..." << std::endl;
    generateAndWriteSpilHitAndWireDataVector(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_vector.root");
    
    std::cout << "Generating Spil HitWire data with Individual format..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividual(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_individual.root");

    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Generating HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteHitWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/vector_dict.root");
    std::cout << "Generating HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteHitWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/individual_dict.root");
    std::cout << "Generating Split HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_vector_dict.root");
    std::cout << "Generating Split HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_individual_dict.root");
    std::cout << "Generating Spil HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataVectorDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_vector_dict.root");
    std::cout << "Generating Spil HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividualDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_individual_dict.root");
    std::cout << "Generating HitWire data with Vector of Individuals format..." << std::endl;
    generateAndWriteHitWireDataVectorOfIndividuals(numEvents, hitsPerEvent, wiresPerEvent, "./output/vector_of_individuals.root");

    return 0;
} 

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..