#pragma once
#include <string>
#include <vector>
#include <utility>
namespace ROOT  { class RNTupleReader; } 
std::vector<std::pair<std::size_t, std::size_t>> split_range_by_clusters(ROOT::RNTupleReader* reader, int nChunks);

void in(int nThreads, int iter = 20);
double read_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_VertiSplit_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_HoriSpill_Hit_Wire_Vector(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_VertiSplit_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_HoriSpill_Hit_Wire_Data_Individual(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_VertiSplit_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_VertiSplit_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_HoriSpill_Hit_Wire_Vector_Dict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_HoriSpill_Hit_Wire_Individual_Dict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads);
double read_Hit_Wire_Vector_Of_Individuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName, int nThreads); 

