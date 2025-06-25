#pragma once
#include <string>

void generateAndWriteHitWireDataVector(int numEvents, int hitsPerEvent, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataVector(int numEvents, int hitsPerEvent, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataVector(int numEvents, int numSpils, int hitsPerEvent, const std::string& fileName);
void generateAndWriteHitWireDataIndividual(int numEvents, int hitsPerEvent, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataIndividual(int numEvents, int hitsPerEvent, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataIndividual(int numEvents, int numSpils, int hitsPerEvent, const std::string& fileName); 