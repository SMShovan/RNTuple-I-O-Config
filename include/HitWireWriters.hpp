#pragma once
#include <string>

void generateAndWriteHitWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataVector(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteHitWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataIndividual(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);

// Dictionary-based experiments
void generateAndWriteHitWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteHitWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataVectorDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataIndividualDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);

void generateAndWriteHitWireDataVectorOfIndividuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName); 