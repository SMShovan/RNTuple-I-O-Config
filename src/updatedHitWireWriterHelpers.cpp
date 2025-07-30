#include "updatedHitWireWriterHelpers.hpp"
#include "HitWireGenerators.hpp"
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

// Data generation (adapt from existing generators)
std::vector<HitIndividual> generateEventHits(long long eventID, int numHits, std::mt19937& rng) {
    std::vector<HitIndividual> hits;
    for (int i = 0; i < numHits; ++i) {
        hits.push_back(generateRandomHitIndividual(eventID, rng)); // Assume this exists or implement
    }
    return hits;
}

std::vector<WireIndividual> generateEventWires(long long eventID, int numWires, int roisPerWire, std::mt19937& rng) {
    std::vector<WireIndividual> wires;
    for (int i = 0; i < numWires; ++i) {
        wires.push_back(generateRandomWireIndividual(eventID, roisPerWire, rng));
    }
    return wires;
}

// Flatten ROIs with WireID (assume WireID from fWire_Channel for simplicity)
std::vector<FlatROI> flattenROIs(const std::vector<WireIndividual>& wires) {
    std::vector<FlatROI> flatROIs;
    for (const auto& wire : wires) {
        unsigned int wireID = wire.fWire_Channel;
        for (const auto& roi : wire.getSignalROI()) {
            FlatROI flat;
            flat.WireID = wireID;
            flat.offset = roi.offset;
            flat.data = roi.data;
            flatROIs.push_back(flat);
        }
    }
    return flatROIs;
}

std::vector<WireROI> flattenWiresToROIs(const std::vector<WireIndividual>& allWires) {
    std::vector<WireROI> wireROIs;
    for (const auto& wire : allWires) {
        for (const auto& roi : wire.getSignalROI()) {
            WireROI wroi;
            wroi.EventID = wire.EventID;
            wroi.fWire_Channel = wire.fWire_Channel;
            wroi.fWire_View = wire.fWire_View;
            wroi.roi = roi;
            wireROIs.push_back(wroi);
        }
    }
    return wireROIs;
}

// Model for allDataProduct: struct EventAOS { std::vector<HitIndividual> hits; std::vector<WireIndividual> wires; }
auto CreateAOSAllDataProductModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<EventAOS>("EventAOS");
    return {std::move(model), model->GetToken("EventAOS")};
}

// Model for perDataProduct: separate hits and wires models
auto CreateAOSHitsModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<std::vector<HitIndividual>>("hits");
    return {std::move(model), model->GetToken("hits")};
}
auto CreateAOSWiresModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<std::vector<WireIndividual>>("wires");
    return {std::move(model), model->GetToken("wires")};
}

// Model for perGroup: hits, wires, rois separately
auto CreateAOSROIsModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<std::vector<FlatROI>>("rois");
    return {std::move(model), model->GetToken("rois")};
}

auto CreateAOSBaseWiresModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<std::vector<WireBase>>("wires");
    return {std::move(model), model->GetToken("wires")};
}

// Work function for allDataProduct
double RunAOS_event_allDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry, ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        EventAOS eventData;
        eventData.hits = generateEventHits(evt, hitsPerEvent, rng);
        eventData.wires = generateEventWires(evt, wiresPerEvent, roisPerWire, rng);
        sw.Start();
        entry.BindRawPtr(token, &eventData);
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
}

// Work function for perDataProduct
double RunAOS_event_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        auto hits = generateEventHits(evt, hitsPerEvent, rng);
        auto wires = generateEventWires(evt, wiresPerEvent, roisPerWire, rng);
        sw.Start();
        // Fill hits
        hitsEntry.BindRawPtr(hitsToken, &hits);
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        // Fill wires
        wiresEntry.BindRawPtr(wiresToken, &wires);
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
}

// Work function for perGroup
double RunAOS_event_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        auto hits = generateEventHits(evt, hitsPerEvent, rng);
        auto wires = generateEventWires(evt, wiresPerEvent, roisPerWire, rng);
        auto rois = flattenROIs(wires);
        std::vector<WireBase> baseWires;
        for(const auto& w : wires) baseWires.push_back(extractWireBase(w));
        sw.Start();
        // Fill hits
        hitsEntry.BindRawPtr(hitsToken, &hits);
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        // Fill wires (without ROIs? or with empty? Assuming wires still include non-ROI fields)
        wiresEntry.BindRawPtr(wiresToken, &baseWires);
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        // Fill rois
        roisEntry.BindRawPtr(roisToken, &rois);
        ROOT::RNTupleFillStatus roisStatus;
        roisContext.FillNoFlush(roisEntry, roisStatus);
        if (roisStatus.ShouldFlushCluster()) {
            roisContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
} 

// Adjusted generation (can reuse existing with adjusted counts)
// No change needed if passing adjusted params

// Work function for spill allDataProduct
double RunAOS_spill_allDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry, ROOT::RFieldToken token, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numSpills;
        int spill = idx % numSpills;
        EventAOS spillData;
        spillData.hits = generateEventHits(evt * numSpills + spill, adjustedHits, rng); // Use unique ID
        spillData.wires = generateEventWires(evt * numSpills + spill, adjustedWires, roisPerWire, rng);
        sw.Start();
        entry.BindRawPtr(token, &spillData);
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
}

// Similar for perDataProduct and perGroup with adjustments
double RunAOS_spill_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numSpills;
        int spill = idx % numSpills;
        auto hits = generateEventHits(evt * numSpills + spill, adjustedHits, rng);
        auto wires = generateEventWires(evt * numSpills + spill, adjustedWires, roisPerWire, rng);
        sw.Start();
        hitsEntry.BindRawPtr(hitsToken, &hits);
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        wiresEntry.BindRawPtr(wiresToken, &wires);
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
}

double RunAOS_spill_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numSpills;
        int spill = idx % numSpills;
        auto hits = generateEventHits(evt * numSpills + spill, adjustedHits, rng);
        auto wires = generateEventWires(evt * numSpills + spill, adjustedWires, roisPerWire, rng);
        auto rois = flattenROIs(wires);
        std::vector<WireBase> baseWires;
        for(const auto& w : wires) baseWires.push_back(extractWireBase(w));
        sw.Start();
        hitsEntry.BindRawPtr(hitsToken, &hits);
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        wiresEntry.BindRawPtr(wiresToken, &baseWires);
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        roisEntry.BindRawPtr(roisToken, &rois);
        ROOT::RNTupleFillStatus roisStatus;
        roisContext.FillNoFlush(roisEntry, roisStatus);
        if (roisStatus.ShouldFlushCluster()) {
            roisContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
} 

// Single generation functions
HitIndividual generateSingleHit(long long id, std::mt19937& rng) {
    return generateRandomHitIndividual(id, rng); // Assume exists
}

WireIndividual generateSingleWire(long long id, int roisPerWire, std::mt19937& rng) {
    return generateRandomWireIndividual(id, roisPerWire, rng);
}

// Struct for topObject allDataProduct (but skipped), for perDataProduct: perhaps pair or separate
// For 3.2: perDataProduct stores single hit and single wire in separate RNTuples per row

// Work for 3.2
double RunAOS_topObject_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        HitIndividual hit = generateSingleHit(idx, rng);
        WireIndividual wire = generateSingleWire(idx, roisPerWire, rng);
        sw.Start();
        hitsEntry.BindRawPtr(hitsToken, &hit);
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        wiresEntry.BindRawPtr(wiresToken, &wire);
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
}

// For 3.3: perGroup with flattened ROIs (multiple per wire)
WireBase extractWireBase(const WireIndividual& wire) {
    WireBase base;
    base.EventID = wire.EventID;
    base.fWire_Channel = wire.fWire_Channel;
    base.fWire_View = wire.fWire_View;
    return base;
}

// Adjust work func for perGroup
double RunAOS_topObject_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        HitIndividual hit = generateSingleHit(idx, rng);
        WireIndividual fullWire = generateSingleWire(idx, roisPerWire, rng);
        WireBase wire = extractWireBase(fullWire);
        auto rois = flattenROIs({fullWire});
        sw.Start();
        hitsEntry.BindRawPtr(hitsToken, &hit);
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        wiresEntry.BindRawPtr(wiresToken, &wire);
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        roisEntry.BindRawPtr(roisToken, &rois);
        ROOT::RNTupleFillStatus roisStatus;
        roisContext.FillNoFlush(roisEntry, roisStatus);
        if (roisStatus.ShouldFlushCluster()) {
            roisContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    return totalTime;
} 

double RunAOS_element_hitsWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto hitPtr = entry.GetPtr<HitIndividual>("hit");
    for (int idx = first; idx < last; ++idx) {
        *hitPtr = generateSingleHit(idx, rng);
        sw.Start();
        context.Fill(entry);
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

double RunAOS_element_wireROIWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto wroiPtr = entry.GetPtr<WireROI>("wire_roi");
    for (int idx = first; idx < last; ++idx) {
        wroiPtr->EventID = idx / roisPerWire;
        wroiPtr->fWire_Channel = rng() % 1024;
        wroiPtr->fWire_View = rng() % 7;
        wroiPtr->roi.offset = rng() % 500;
        wroiPtr->roi.data.resize(10);
        for (auto& val : wroiPtr->roi.data) val = static_cast<float>(rng() % 100);
        sw.Start();
        context.Fill(entry);
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

double RunAOS_element_wiresWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto wirePtr = entry.GetPtr<WireBase>("wire");
    for (int idx = first; idx < last; ++idx) {
        wirePtr->EventID = idx;
        wirePtr->fWire_Channel = rng() % 1024;
        wirePtr->fWire_View = rng() % 7;
        sw.Start();
        context.Fill(entry);
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

double RunAOS_element_roisWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto roiPtr = entry.GetPtr<FlatROI>("roi");
    for (int idx = first; idx < last; ++idx) {
        roiPtr->WireID = idx / roisPerWire;
        roiPtr->offset = rng() % 500;
        roiPtr->data.resize(10);
        for (auto& val : roiPtr->data) val = static_cast<float>(rng() % 100);
        sw.Start();
        context.Fill(entry);
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
} 