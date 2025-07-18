#pragma once
#include <string>
#include <vector>
#include <utility>
namespace ROOT  { class RNTupleReader; } 
std::vector<std::pair<std::size_t, std::size_t>> split_range_by_clusters(ROOT::RNTupleReader* reader, int nChunks);

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

