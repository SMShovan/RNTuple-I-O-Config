#pragma once
#include "Hit.hpp"
#include "Wire.hpp"
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RFieldToken.hxx>
#include <ROOT/RRawPtrWriteEntry.hxx>
#include <ROOT/RNTupleFillContext.hxx>
#include <ROOT/RNTupleFillStatus.hxx>
#include <unordered_map>
#include <random>
#include <vector>
#include <mutex>
#include <TStopwatch.h>

struct FlatROI {
    unsigned int WireID;
    std::size_t offset;
    std::vector<float> data;
    ClassDef(FlatROI, 1);
};

std::vector<HitIndividual> generateEventHits(long long eventID, int numHits, std::mt19937& rng);
std::vector<WireIndividual> generateEventWires(long long eventID, int numWires, int roisPerWire, std::mt19937& rng);
std::vector<FlatROI> flattenROIs(const std::vector<WireIndividual>& wires);

struct EventAOS {
    std::vector<HitIndividual> hits;
    std::vector<WireIndividual> wires;
    ClassDef(EventAOS, 1);
};

auto CreateAOSAllDataProductModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;
auto CreateAOSHitsModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;
auto CreateAOSWiresModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;
auto CreateAOSROIsModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;
auto CreateAOSBaseWiresModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

double RunAOS_event_allDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry, ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire);
double RunAOS_event_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire);
double RunAOS_event_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire); 
double RunAOS_spill_allDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry, ROOT::RFieldToken token, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire);
double RunAOS_spill_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire);
double RunAOS_spill_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire); 

HitIndividual generateSingleHit(long long id, std::mt19937& rng);
WireIndividual generateSingleWire(long long id, int roisPerWire, std::mt19937& rng);

double RunAOS_topObject_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int roisPerWire);
double RunAOS_topObject_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int roisPerWire); 

struct WireBase {
    long long EventID;
    unsigned int fWire_Channel;
    int fWire_View;
    ClassDef(WireBase, 1);
};

WireBase extractWireBase(const WireIndividual& wire); 

struct WireROI {
    long long EventID;
    unsigned int fWire_Channel;
    int fWire_View;
    RegionOfInterest roi;
    ClassDef(WireROI, 1);
};

std::vector<WireROI> flattenWiresToROIs(const std::vector<WireIndividual>& wires); 

double RunAOS_element_hitsWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex);
double RunAOS_element_wireROIWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire);
double RunAOS_element_wiresWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex);
double RunAOS_element_roisWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire); 