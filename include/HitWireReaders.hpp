#pragma once
#include <string>

void in();
void readHitWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSplitHitAndWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSpilHitAndWireDataVector(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readHitWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSplitHitAndWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSpilHitAndWireDataIndividual(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readHitWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readHitWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSplitHitAndWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSplitHitAndWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSpilHitAndWireDataVectorDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readSpilHitAndWireDataIndividualDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName);
void readHitWireDataVectorOfIndividuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName); 

