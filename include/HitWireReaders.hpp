#pragma once
#include <string>

void in(int nThreads);
void readHitWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSplitHitAndWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSpilHitAndWireDataVector(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readHitWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSplitHitAndWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSpilHitAndWireDataIndividual(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readHitWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readHitWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSplitHitAndWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSplitHitAndWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSpilHitAndWireDataVectorDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readSpilHitAndWireDataIndividualDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
void readHitWireDataVectorOfIndividuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads); 

