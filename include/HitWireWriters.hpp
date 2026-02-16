#pragma once
#include <string>
#include <vector>
#include <map>
#include <utility>
#include "WriterResult.hpp"

// Group 1: Event-level
double AOS_event_allDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double AOS_event_perDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double AOS_event_perGroup(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);

// Group 2: Spill-level
double AOS_spill_allDataProduct(int numEvents, int numSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double AOS_spill_perDataProduct(int numEvents, int numSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double AOS_spill_perGroup(int numEvents, int numSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);

// Group 3: TopObject-level (skip 3.1)
double AOS_topObject_perDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double AOS_topObject_perGroup(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double AOS_topObject_allDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);

// Group 4: Element-level (skip 4.1)
double AOS_element_perDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double AOS_element_perGroup(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads); 
double AOS_element_allDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);

std::vector<WriterResult> outAOS(int nThreads, int iter, int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, int numSpills, const std::string& outputDir, int mask = -1, bool measureWallTime = false);
std::vector<WriterResult> outSOA(int nThreads, int iter, int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, int numSpills, const std::string& outputDir, int mask = -1, bool measureWallTime = false);
std::map<std::string, std::vector<std::pair<int, double>>> benchmarkAOSScaling(int maxThreads, int iter, int numEvents, int mask);
std::map<std::string, std::vector<std::pair<int, double>>> benchmarkSOAScaling(int maxThreads, int iter, int numEvents, int mask);

// SOA top/element allDataProduct
double SOA_topObject_allDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);
double SOA_element_allDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads);