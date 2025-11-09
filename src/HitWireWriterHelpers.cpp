#include "HitWireWriterHelpers.hpp"
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
#include "UnionRow.hpp"
#include "UnionRowSOA.hpp"
#include "TopBatchRow.hpp"
#include "TopBatchRowSOA.hpp"
#include "Utils.hpp"

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

// Deterministic per-entry generators (thread/config independent)
std::vector<HitIndividual> generateEventHitsDeterministic(long long eventID, int numHits) {
    std::vector<HitIndividual> hits;
    hits.reserve(numHits);
    for (int i = 0; i < numHits; ++i) {
        std::uint32_t seed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(eventID), static_cast<std::uint64_t>(i));
        std::mt19937 rngLocal(seed);
        hits.push_back(generateRandomHitIndividual(eventID, rngLocal));
    }
    return hits;
}

std::vector<WireIndividual> generateEventWiresDeterministic(long long eventID, int numWires, int roisPerWire) {
    std::vector<WireIndividual> wires;
    wires.reserve(numWires);
    for (int w = 0; w < numWires; ++w) {
        std::uint32_t seed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(eventID), static_cast<std::uint64_t>(w));
        std::mt19937 rngLocal(seed);
        wires.push_back(generateRandomWireIndividual(eventID, roisPerWire, rngLocal));
    }
    return wires;
}

// Deterministic range variants (useful for spills slicing a single event)
std::vector<HitIndividual> generateEventHitsDeterministicRange(long long eventID, int startIndex, int count) {
    std::vector<HitIndividual> hits;
    hits.reserve(count);
    for (int i = 0; i < count; ++i) {
        int idx = startIndex + i;
        std::uint32_t seed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(eventID), static_cast<std::uint64_t>(idx));
        std::mt19937 rngLocal(seed);
        hits.push_back(generateRandomHitIndividual(eventID, rngLocal));
    }
    return hits;
}

std::vector<WireIndividual> generateEventWiresDeterministicRange(long long eventID, int startIndex, int count, int roisPerWire) {
    std::vector<WireIndividual> wires;
    wires.reserve(count);
    for (int i = 0; i < count; ++i) {
        int idx = startIndex + i;
        std::uint32_t seed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(eventID), static_cast<std::uint64_t>(idx));
        std::mt19937 rngLocal(seed);
        wires.push_back(generateRandomWireIndividual(eventID, roisPerWire, rngLocal));
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
            flat.EventID = wire.EventID;
            flat.WireID  = wireID;
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

// Union models for allDataProduct (per-top/element rows)
auto CreateAOSUnionModelAndToken(const std::string& fieldName) -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<AOSUnionRow>(fieldName.c_str());
    return {std::move(model), model->GetToken(fieldName.c_str())};
}

auto CreateSOAUnionModelAndToken(const std::string& fieldName) -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<SOAUnionRow>(fieldName.c_str());
    return {std::move(model), model->GetToken(fieldName.c_str())};
}

auto CreateAOSTopBatchModelAndToken(const std::string& fieldName) -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<AOSTopBatchRow>(fieldName.c_str());
    return {std::move(model), model->GetToken(fieldName.c_str())};
}

auto CreateSOATopBatchModelAndToken(const std::string& fieldName) -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<SOATopBatchRow>(fieldName.c_str());
    return {std::move(model), model->GetToken(fieldName.c_str())};
}

// AOS top-batch work: K rows per event (K = max(H, W)), optional hit/wire
double RunAOS_top_allDataProductWorkFunc(int firstEvt, int lastEvt, unsigned /*seed*/,
    ROOT::Experimental::RNTupleFillContext& ctx, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry,
    ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw; double totalTime = 0.0;
    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        const int K = (hitsPerEvent > wiresPerEvent) ? hitsPerEvent : wiresPerEvent;
        for (int k = 0; k < K; ++k) {
            AOSTopBatchRow row{}; row.EventID = static_cast<unsigned int>(evt);
            if (k < hitsPerEvent) {
                std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(k));
                std::mt19937 hRng(hSeed);
                row.hasHit = true;
                row.hit = generateRandomHitIndividual(static_cast<long long>(evt) * hitsPerEvent + k, hRng);
            } else {
                row.hasHit = false;
            }
            if (k < wiresPerEvent) {
                std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(k));
                std::mt19937 wRng(wSeed);
                WireIndividual wi = generateRandomWireIndividual(evt, roisPerWire, wRng);
                row.hasWire = true;
                row.wire = extractWireBase(wi);
                row.rois.clear();
                row.rois.reserve(roisPerWire);
                for (int r = 0; r < roisPerWire; ++r) {
                    FlatROI flat{};
                    flat.EventID = static_cast<unsigned int>(evt);
                    flat.WireID  = wi.fWire_Channel;
                    flat.offset  = wi.getSignalROI()[r].offset;
                    flat.data    = wi.getSignalROI()[r].data;
                    row.rois.push_back(std::move(flat));
                }
            } else {
                row.hasWire = false;
                row.rois.clear();
            }
            sw.Start();
            entry.BindRawPtr(token, &row);
            { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st);
              if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } }
            totalTime += sw.RealTime();
        }
    }
    return totalTime;
}

// AOS union work: 1 fill per element (hit, wire, ROI elements)
double RunAOS_element_allDataProductWorkFunc(int firstEvt, int lastEvt, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& ctx, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry,
    ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw; double totalTime = 0.0;
    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        // Hit elements
        for (int h = 0; h < hitsPerEvent; ++h) {
            std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(h));
            std::mt19937 hRng(hSeed);
            HitIndividual hi = generateRandomHitIndividual(static_cast<long long>(evt) * hitsPerEvent + h, hRng);
            AOSUnionRow row{}; row.EventID = evt; row.recordType = 0; row.WireID = 0; row.hit = hi;
            sw.Start(); entry.BindRawPtr(token, &row);
            { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st); if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } }
            totalTime += sw.RealTime();
        }
        // Wire elements and their ROI elements
        for (int w = 0; w < wiresPerEvent; ++w) {
            std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(w));
            std::mt19937 wRng(wSeed);
            WireIndividual wi = generateRandomWireIndividual(evt, roisPerWire, wRng);
            // Wire element row
            AOSUnionRow rowW{}; rowW.EventID = evt; rowW.recordType = 1; rowW.WireID = wi.fWire_Channel; rowW.wire = extractWireBase(wi);
            sw.Start(); entry.BindRawPtr(token, &rowW);
            { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st); if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } }
            totalTime += sw.RealTime();
            // ROI element rows
            for (int r = 0; r < roisPerWire; ++r) {
                AOSUnionRow rowR{}; rowR.EventID = evt; rowR.recordType = 2; rowR.WireID = wi.fWire_Channel;
                rowR.roi.EventID = evt; rowR.roi.WireID = wi.fWire_Channel; rowR.roi.offset = wi.getSignalROI()[r].offset; rowR.roi.data = wi.getSignalROI()[r].data;
                sw.Start(); entry.BindRawPtr(token, &rowR);
                { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st); if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } }
                totalTime += sw.RealTime();
            }
        }
    }
    return totalTime;
}

// SOA top-batch work: K rows per event (K = max(H, W)), optional hit/wire
double RunSOA_top_allDataProductWorkFunc(int firstEvt, int lastEvt, unsigned /*seed*/,
    ROOT::Experimental::RNTupleFillContext& ctx, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry,
    ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw; double totalTime = 0.0;
    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        const int K = (hitsPerEvent > wiresPerEvent) ? hitsPerEvent : wiresPerEvent;
        for (int k = 0; k < K; ++k) {
            SOATopBatchRow row{}; row.EventID = static_cast<unsigned int>(evt);
            if (k < hitsPerEvent) {
                std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(k));
                std::mt19937 hRng(hSeed);
                HitIndividual hInd = generateRandomHitIndividual(static_cast<long long>(evt) * hitsPerEvent + k, hRng);
                row.hasHit = true;
                row.hit.EventID = hInd.EventID;
                row.hit.Channel = hInd.fChannel;
                row.hit.View = hInd.fView;
                row.hit.StartTick = hInd.fStartTick;
                row.hit.EndTick = hInd.fEndTick;
                row.hit.PeakTime = hInd.fPeakTime;
                row.hit.SigmaPeakTime = hInd.fSigmaPeakTime;
                row.hit.RMS = hInd.fRMS;
                row.hit.PeakAmplitude = hInd.fPeakAmplitude;
                row.hit.SigmaPeakAmplitude = hInd.fSigmaPeakAmplitude;
                row.hit.ROISummedADC = hInd.fROISummedADC;
                row.hit.HitSummedADC = hInd.fHitSummedADC;
                row.hit.Integral = hInd.fIntegral;
                row.hit.SigmaIntegral = hInd.fSigmaIntegral;
                row.hit.Multiplicity = hInd.fMultiplicity;
                row.hit.LocalIndex = hInd.fLocalIndex;
                row.hit.GoodnessOfFit = hInd.fGoodnessOfFit;
                row.hit.NDF = hInd.fNDF;
                row.hit.SignalType = hInd.fSignalType;
                row.hit.WireID_Cryostat = hInd.fWireID_Cryostat;
                row.hit.WireID_TPC = hInd.fWireID_TPC;
                row.hit.WireID_Plane = hInd.fWireID_Plane;
                row.hit.WireID_Wire = hInd.fWireID_Wire;
            } else {
                row.hasHit = false;
            }
            if (k < wiresPerEvent) {
                std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(k));
                std::mt19937 wRng(wSeed);
                WireIndividual wInd = generateRandomWireIndividual(evt, roisPerWire, wRng);
                row.hasWire = true;
                row.wire.EventID = evt;
                row.wire.Channel = wInd.fWire_Channel;
                row.wire.View = wInd.fWire_View;
                row.rois.clear();
                row.rois.resize(roisPerWire);
                for (int r = 0; r < roisPerWire; ++r) {
                    row.rois[r].data = wInd.getSignalROI()[r].data;
                }
            } else {
                row.hasWire = false;
                row.rois.clear();
            }
            sw.Start();
            entry.BindRawPtr(token, &row);
            { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st);
              if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } }
            totalTime += sw.RealTime();
        }
    }
    return totalTime;
}

// SOA union work: 1 fill per element
double RunSOA_element_allDataProductWorkFunc(int firstEvt, int lastEvt, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& ctx, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry,
    ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw; double totalTime = 0.0;
    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        for (int h = 0; h < hitsPerEvent; ++h) {
            std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(h));
            std::mt19937 hRng(hSeed);
            HitIndividual hInd = generateRandomHitIndividual(static_cast<long long>(evt) * hitsPerEvent + h, hRng);
            SOAUnionRow row{}; row.EventID = evt; row.recordType = 0; row.WireID = 0;
            row.hit.EventID = hInd.EventID; row.hit.Channel = hInd.fChannel; row.hit.View = hInd.fView;
            row.hit.StartTick = hInd.fStartTick; row.hit.EndTick = hInd.fEndTick; row.hit.PeakTime = hInd.fPeakTime; row.hit.SigmaPeakTime = hInd.fSigmaPeakTime;
            row.hit.RMS = hInd.fRMS; row.hit.PeakAmplitude = hInd.fPeakAmplitude; row.hit.SigmaPeakAmplitude = hInd.fSigmaPeakAmplitude;
            row.hit.ROISummedADC = hInd.fROISummedADC; row.hit.HitSummedADC = hInd.fHitSummedADC; row.hit.Integral = hInd.fIntegral; row.hit.SigmaIntegral = hInd.fSigmaIntegral;
            row.hit.Multiplicity = hInd.fMultiplicity; row.hit.LocalIndex = hInd.fLocalIndex; row.hit.GoodnessOfFit = hInd.fGoodnessOfFit;
            row.hit.NDF = hInd.fNDF; row.hit.SignalType = hInd.fSignalType; row.hit.WireID_Cryostat = hInd.fWireID_Cryostat; row.hit.WireID_TPC = hInd.fWireID_TPC; row.hit.WireID_Plane = hInd.fWireID_Plane; row.hit.WireID_Wire = hInd.fWireID_Wire;
            sw.Start(); entry.BindRawPtr(token, &row); { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st); if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } } totalTime += sw.RealTime();
        }
        for (int w = 0; w < wiresPerEvent; ++w) {
            std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(w));
            std::mt19937 wRng(wSeed);
            WireIndividual wInd = generateRandomWireIndividual(evt, roisPerWire, wRng);
            SOAUnionRow rowW{}; rowW.EventID = evt; rowW.recordType = 1; rowW.WireID = wInd.fWire_Channel; rowW.wire.EventID = evt; rowW.wire.Channel = wInd.fWire_Channel; rowW.wire.View = wInd.fWire_View;
            sw.Start(); entry.BindRawPtr(token, &rowW); { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st); if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } } totalTime += sw.RealTime();
            for (int r = 0; r < roisPerWire; ++r) {
                SOAUnionRow rowR{}; rowR.EventID = evt; rowR.recordType = 2; rowR.WireID = wInd.fWire_Channel; rowR.roi.EventID = evt; rowR.roi.WireID = wInd.fWire_Channel; rowR.roi.offset = wInd.getSignalROI()[r].offset; rowR.roi.data = wInd.getSignalROI()[r].data;
                sw.Start(); entry.BindRawPtr(token, &rowR); { ROOT::RNTupleFillStatus st; ctx.FillNoFlush(entry, st); if (st.ShouldFlushCluster()) { ctx.FlushColumns(); std::lock_guard<std::mutex> lk(mutex); ctx.FlushCluster(); } } totalTime += sw.RealTime();
            }
        }
    }
    return totalTime;
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
double RunAOS_event_allDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry, ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire,
    double* outDataGen, double* outSerialize, double* outFlushColumns, double* outFlushCluster) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double dataGenTime = 0.0, serializeTime = 0.0, flushColumnsTime = 0.0, flushClusterTime = 0.0;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        // W1 — Data generation
        {
            TStopwatch swPhase; swPhase.Start();
            EventAOS eventData;
            eventData.hits = generateEventHitsDeterministic(evt, hitsPerEvent);
            eventData.wires = generateEventWiresDeterministic(evt, wiresPerEvent, roisPerWire);

            dataGenTime += swPhase.RealTime();

            // W2 — Serialize/fill
            sw.Start();
            TStopwatch swSer; swSer.Start();
            entry.BindRawPtr(token, &eventData);
            ROOT::RNTupleFillStatus status;
            context.FillNoFlush(entry, status);
            serializeTime += swSer.RealTime();

            // W3 — Flush columns (no lock) and W4 — Flush cluster (with lock)
            if (status.ShouldFlushCluster()) {
                TStopwatch swFC; swFC.Start();
                context.FlushColumns();
                flushColumnsTime += swFC.RealTime();
                TStopwatch swFCl; swFCl.Start();
                { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
                flushClusterTime += swFCl.RealTime();
            }
            totalTime += sw.RealTime();
        }
    }
    if (outDataGen) *outDataGen = dataGenTime;
    if (outSerialize) *outSerialize = serializeTime;
    if (outFlushColumns) *outFlushColumns = flushColumnsTime;
    if (outFlushCluster) *outFlushCluster = flushClusterTime;
    return totalTime;
}

// Work function for perDataProduct
double RunAOS_event_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        // Deterministic content independent of thread/config
        auto hits = generateEventHitsDeterministic(evt, hitsPerEvent);
        auto wires = generateEventWiresDeterministic(evt, wiresPerEvent, roisPerWire);
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
        // Deterministic content independent of thread/config
        auto hits = generateEventHitsDeterministic(evt, hitsPerEvent);
        auto wires = generateEventWiresDeterministic(evt, wiresPerEvent, roisPerWire);
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
        int startHit = spill * adjustedHits;
        int startWire = spill * adjustedWires;
        EventAOS spillData;
        // Deterministic slices from the same event content
        spillData.hits = generateEventHitsDeterministicRange(evt, startHit, adjustedHits);
        spillData.wires = generateEventWiresDeterministicRange(evt, startWire, adjustedWires, roisPerWire);
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
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numSpills;
        int spill = idx % numSpills;
        int startHit = spill * adjustedHits;
        int startWire = spill * adjustedWires;
        // Deterministic slices from event-level content
        auto hits = generateEventHitsDeterministicRange(evt, startHit, adjustedHits);
        auto wires = generateEventWiresDeterministicRange(evt, startWire, adjustedWires, roisPerWire);
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
// Adjust RunAOS_topObject_perDataProductWorkFunc and RunAOS_topObject_perGroupWorkFunc to use REntry: GetPtr, set values, context.Fill(entry)

double RunAOS_topObject_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::REntry& wiresEntry, std::mutex& mutex, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;
    auto hitPtr = hitsEntry.GetPtr<HitIndividual>("hit");
    auto wirePtr = wiresEntry.GetPtr<WireIndividual>("wire");
    for (int idx = first; idx < last; ++idx) {
        // Deterministic per-entry generation
        std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(idx), static_cast<std::uint64_t>(0));
        std::mt19937 hRng(hSeed);
        HitIndividual hit = generateRandomHitIndividual(idx, hRng);
        std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(idx), static_cast<std::uint64_t>(0));
        std::mt19937 wRng(wSeed);
        WireIndividual wire = generateRandomWireIndividual(idx, roisPerWire, wRng);
        sw.Start();
        *hitPtr = hit;
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        *wirePtr = wire;
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    { std::lock_guard<std::mutex> lock(mutex);
      hitsContext.FlushCluster();
      wiresContext.FlushCluster(); }
    return totalTime;
}

double RunAOS_topObject_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::REntry& wiresEntry, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::REntry& roisEntry, std::mutex& mutex, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;
    auto hitPtr = hitsEntry.GetPtr<HitIndividual>("hit");
    auto wirePtr = wiresEntry.GetPtr<WireBase>("wire");
    auto roisPtr = roisEntry.GetPtr<std::vector<FlatROI>>("rois");
    for (int idx = first; idx < last; ++idx) {
        // Deterministic per-entry generation
        std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(idx), static_cast<std::uint64_t>(0));
        std::mt19937 hRng(hSeed);
        HitIndividual hit = generateRandomHitIndividual(idx, hRng);
        std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(idx), static_cast<std::uint64_t>(0));
        std::mt19937 wRng(wSeed);
        WireIndividual fullWire = generateRandomWireIndividual(idx, roisPerWire, wRng);
        WireBase wire = extractWireBase(fullWire);
        auto rois = flattenROIs({fullWire});
        sw.Start();
        *hitPtr = hit;
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        *wirePtr = wire;
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        *roisPtr = rois;
        ROOT::RNTupleFillStatus roisStatus;
        roisContext.FillNoFlush(roisEntry, roisStatus);
        if (roisStatus.ShouldFlushCluster()) {
            roisContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    { std::lock_guard<std::mutex> lock(mutex);
      hitsContext.FlushCluster();
      wiresContext.FlushCluster();
      roisContext.FlushCluster(); }
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



double RunAOS_element_hitsWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto hitPtr = entry.GetPtr<HitIndividual>("hit");
    for (int idx = first; idx < last; ++idx) {
        *hitPtr = generateSingleHit(idx, rng);
        sw.Start();
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

double RunAOS_element_wireROIWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    std::uniform_real_distribution<float> distADC(0.0f, 100.0f);
    double totalTime = 0.0;
    auto wroiPtr = entry.GetPtr<WireROI>("wire_roi");
    for (int idx = first; idx < last; ++idx) {
        wroiPtr->EventID = idx / roisPerWire;
        wroiPtr->fWire_Channel = rng() % 1024;
        wroiPtr->fWire_View = rng() % 7;
        wroiPtr->roi.offset = rng() % 500;
        wroiPtr->roi.data.resize(10);
        for (auto& val : wroiPtr->roi.data) val = distADC(rng);
        sw.Start();
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
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
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

double RunAOS_element_roisWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    std::uniform_real_distribution<float> distADC(0.0f, 100.0f);
    double totalTime = 0.0;
    auto roiPtr = entry.GetPtr<FlatROI>("roi");
    for (int idx = first; idx < last; ++idx) {
        unsigned int eventID = idx / (roisPerWire * 1000000); // rough grouping; adjust as needed
        unsigned int wireIdx = (idx / roisPerWire) % 1000000;
        roiPtr->EventID = eventID;
        roiPtr->WireID  = wireIdx;
        roiPtr->offset  = rng() % 500;
        roiPtr->data.resize(10);
        for (auto& val : roiPtr->data) val = distADC(rng);
        sw.Start();
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

// Combined hits + wireROI per-event work function for element_perDataProduct (single pass)
double RunAOS_element_perDataProductCombinedWorkFunc(int firstEvt, int lastEvt, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry,
    ROOT::Experimental::RNTupleFillContext& wireROIContext, ROOT::REntry& wireROIEntry,
    std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;

    auto hitPtr   = hitsEntry.GetPtr<HitIndividual>("hit");
    auto wroiPtr  = wireROIEntry.GetPtr<WireROI>("wire_roi");

    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        // hits (deterministic per-entry)
        for (int h = 0; h < hitsPerEvent; ++h) {
            long long gid = static_cast<long long>(evt) * hitsPerEvent + h;
            std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(h));
            std::mt19937 hRng(hSeed);
            *hitPtr = generateRandomHitIndividual(gid, hRng);
            sw.Start();
            ROOT::RNTupleFillStatus hitStatus;
            hitsContext.FillNoFlush(hitsEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitsContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
            }
            totalTime += sw.RealTime();
        }
        // wire ROIs (deterministic per wire and ROI)
        for (int w = 0; w < wiresPerEvent; ++w) {
            // Build deterministic wire to source channel/view and ROI count
            std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(w));
            std::mt19937 wRng(wSeed);
            WireIndividual wInd = generateRandomWireIndividual(evt, roisPerWire, wRng);
            for (int r = 0; r < roisPerWire; ++r) {
                wroiPtr->EventID       = evt;
                wroiPtr->fWire_Channel = wInd.fWire_Channel;
                wroiPtr->fWire_View    = wInd.fWire_View;
                wroiPtr->roi.offset    = wInd.getSignalROI()[r].offset;
                wroiPtr->roi.data      = wInd.getSignalROI()[r].data;
                sw.Start();
                ROOT::RNTupleFillStatus wroiStatus;
                wireROIContext.FillNoFlush(wireROIEntry, wroiStatus);
                if (wroiStatus.ShouldFlushCluster()) {
                    wireROIContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); wireROIContext.FlushCluster(); }
                }
                totalTime += sw.RealTime();
            }
        }
    }
    // Flush once per thread to minimise contention
    {
        std::lock_guard<std::mutex> lock(mutex);
        hitsContext.FlushCluster();
        wireROIContext.FlushCluster();
    }
    return totalTime;
}

// Combined hits + wires + ROIs per-event work function for element_perGroup (single pass)

// Combined hits + wireROI per-event work function for SOA_element_perDataProduct (single pass)
double RunSOA_element_perDataProductCombinedWorkFunc(int firstEvt, int lastEvt, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry,
    ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::REntry& roisEntry,
    std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;

    auto hitPtr  = hitsEntry.GetPtr<SOAHit>("hit");
    auto roiPtr  = roisEntry.GetPtr<FlatSOAROI>("roi");

    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        for (int h = 0; h < hitsPerEvent; ++h) {
            long long gid = static_cast<long long>(evt) * hitsPerEvent + h;
            std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(h));
            std::mt19937 hRng(hSeed);
            HitIndividual hInd = generateRandomHitIndividual(gid, hRng);
            SOAHit hit;
            hit.EventID = hInd.EventID;
            hit.Channel = hInd.fChannel;
            hit.View = hInd.fView;
            hit.StartTick = hInd.fStartTick;
            hit.EndTick = hInd.fEndTick;
            hit.PeakTime = hInd.fPeakTime;
            hit.SigmaPeakTime = hInd.fSigmaPeakTime;
            hit.RMS = hInd.fRMS;
            hit.PeakAmplitude = hInd.fPeakAmplitude;
            hit.SigmaPeakAmplitude = hInd.fSigmaPeakAmplitude;
            hit.ROISummedADC = hInd.fROISummedADC;
            hit.HitSummedADC = hInd.fHitSummedADC;
            hit.Integral = hInd.fIntegral;
            hit.SigmaIntegral = hInd.fSigmaIntegral;
            hit.Multiplicity = hInd.fMultiplicity;
            hit.LocalIndex = hInd.fLocalIndex;
            hit.GoodnessOfFit = hInd.fGoodnessOfFit;
            hit.NDF = hInd.fNDF;
            hit.SignalType = hInd.fSignalType;
            hit.WireID_Cryostat = hInd.fWireID_Cryostat;
            hit.WireID_TPC = hInd.fWireID_TPC;
            hit.WireID_Plane = hInd.fWireID_Plane;
            hit.WireID_Wire = hInd.fWireID_Wire;
            *hitPtr = hit;
            sw.Start();
            ROOT::RNTupleFillStatus hitStatus;
            hitsContext.FillNoFlush(hitsEntry, hitStatus);
            totalTime += sw.RealTime();
            if (hitStatus.ShouldFlushCluster()) {
                hitsContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
            }
        }
        for (int w = 0; w < wiresPerEvent; ++w) {
            // Deterministic wire/ROI mapping to FlatSOAROI
            std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(w));
            std::mt19937 wRng(wSeed);
            WireIndividual wInd = generateRandomWireIndividual(evt, roisPerWire, wRng);
            for (int r = 0; r < roisPerWire; ++r) {
                roiPtr->EventID = static_cast<unsigned int>(evt);
                roiPtr->WireID  = static_cast<unsigned int>(wInd.fWire_Channel);
                roiPtr->data    = wInd.getSignalROI()[r].data;
                sw.Start();
                ROOT::RNTupleFillStatus roiStatus;
                roisContext.FillNoFlush(roisEntry, roiStatus);
                totalTime += sw.RealTime();
                if (roiStatus.ShouldFlushCluster()) {
                    roisContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
                }
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        hitsContext.FlushCluster();
        roisContext.FlushCluster();
    }
    return totalTime;
}

// Combined hits + wires + ROIs per-event work function for element_perGroup (single pass)
double RunAOS_element_perGroupCombinedWorkFunc(int firstEvt, int lastEvt, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry,
    ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::REntry& wiresEntry,
    ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::REntry& roisEntry,
    std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;

    auto hitPtr  = hitsEntry.GetPtr<HitIndividual>("hit");
    auto wirePtr = wiresEntry.GetPtr<WireBase>("wire");
    auto roiPtr  = roisEntry.GetPtr<FlatROI>("roi");

    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        // hits (deterministic per-entry)
        for (int h = 0; h < hitsPerEvent; ++h) {
            long long gid = static_cast<long long>(evt) * hitsPerEvent + h;
            std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(h));
            std::mt19937 hRng(hSeed);
            *hitPtr = generateRandomHitIndividual(gid, hRng);
            sw.Start();
            ROOT::RNTupleFillStatus hitStatus;
            hitsContext.FillNoFlush(hitsEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitsContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
            }
            totalTime += sw.RealTime();
        }
        // wires & ROIs (deterministic per wire and ROI)
        for (int w = 0; w < wiresPerEvent; ++w) {
            std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(evt), static_cast<std::uint64_t>(w));
            std::mt19937 wRng(wSeed);
            WireIndividual wInd = generateRandomWireIndividual(evt, roisPerWire, wRng);
            // base wire
            wirePtr->EventID = evt;
            wirePtr->fWire_Channel = wInd.fWire_Channel;
            wirePtr->fWire_View    = wInd.fWire_View;
            sw.Start();
            ROOT::RNTupleFillStatus wireStatus;
            wiresContext.FillNoFlush(wiresEntry, wireStatus);
            totalTime += sw.RealTime();
            if (wireStatus.ShouldFlushCluster()) {
                wiresContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
            }

            // its ROIs (deterministic)
            for (int r = 0; r < roisPerWire; ++r) {
                roiPtr->EventID = evt;
                roiPtr->WireID  = wInd.fWire_Channel;
                roiPtr->offset  = wInd.getSignalROI()[r].offset;
                roiPtr->data    = wInd.getSignalROI()[r].data;
                sw.Start();
                ROOT::RNTupleFillStatus roiStatus;
                roisContext.FillNoFlush(roisEntry, roiStatus);
                totalTime += sw.RealTime();
                if (roiStatus.ShouldFlushCluster()) {
                    roisContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
                }
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        hitsContext.FlushCluster();
        wiresContext.FlushCluster();
        roisContext.FlushCluster();
    }
    return totalTime;
}



// Combined hits + wires + ROIs per-event work function for SOA_element_perGroup (single pass)
double RunSOA_element_perGroupCombinedWorkFunc(int firstEvt, int lastEvt, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry,
    ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::REntry& wiresEntry,
    ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::REntry& roisEntry,
    std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;

    auto hitPtr  = hitsEntry.GetPtr<SOAHit>("hit");
    auto wirePtr = wiresEntry.GetPtr<SOAWireBase>("wire");
    auto roiPtr  = roisEntry.GetPtr<FlatSOAROI>("roi");

    for (int evt = firstEvt; evt < lastEvt; ++evt) {
        for (int h = 0; h < hitsPerEvent; ++h) {
            *hitPtr = generateSOASingleHit(static_cast<long long>(evt) * hitsPerEvent + h, rng);
            sw.Start();
            ROOT::RNTupleFillStatus hitStatus;
            hitsContext.FillNoFlush(hitsEntry, hitStatus);
            totalTime += sw.RealTime();
            if (hitStatus.ShouldFlushCluster()) {
                hitsContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
            }
        }
        for (int w = 0; w < wiresPerEvent; ++w) {
            wirePtr->EventID = evt;
            wirePtr->Channel = rng() % 1024;
            wirePtr->View    = rng() % 7;
            sw.Start();
            ROOT::RNTupleFillStatus wireStatus;
            wiresContext.FillNoFlush(wiresEntry, wireStatus);
            totalTime += sw.RealTime();
            if (wireStatus.ShouldFlushCluster()) {
                wiresContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
            }
            for (int r = 0; r < roisPerWire; ++r) {
                *roiPtr = generateSOASingleROI(evt, w, rng);
                sw.Start();
                ROOT::RNTupleFillStatus roiStatus;
                roisContext.FillNoFlush(roisEntry, roiStatus);
                totalTime += sw.RealTime();
                if (roiStatus.ShouldFlushCluster()) {
                    roisContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
                }
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        hitsContext.FlushCluster();
        wiresContext.FlushCluster();
        roisContext.FlushCluster();
    }
    return totalTime;
}

// SOA Data Generation
SOAHitVector generateSOAEventHits(long long eventID, int numHits, std::mt19937& rng) {
    SOAHitVector hits;
    hits.EventIDs.resize(numHits, eventID);
    hits.Channels.resize(numHits);
    hits.Views.resize(numHits);
    hits.StartTicks.resize(numHits);
    hits.EndTicks.resize(numHits);
    hits.PeakTimes.resize(numHits);
    hits.SigmaPeakTimes.resize(numHits);
    hits.RMSs.resize(numHits);
    hits.PeakAmplitudes.resize(numHits);
    hits.SigmaPeakAmplitudes.resize(numHits);
    hits.ROISummedADCs.resize(numHits);
    hits.HitSummedADCs.resize(numHits);
    hits.Integrals.resize(numHits);
    hits.SigmaIntegrals.resize(numHits);
    hits.Multiplicities.resize(numHits);
    hits.LocalIndices.resize(numHits);
    hits.GoodnessOfFits.resize(numHits);
    hits.NDFs.resize(numHits);
    hits.SignalTypes.resize(numHits);
    hits.WireID_Cryostats.resize(numHits);
    hits.WireID_TPCs.resize(numHits);
    hits.WireID_Planes.resize(numHits);
    hits.WireID_Wires.resize(numHits);
    for (int i = 0; i < numHits; ++i) {
        HitIndividual temp = generateRandomHitIndividual(eventID, rng);
        hits.Channels[i] = temp.fChannel;
        hits.Views[i] = temp.fView;
        hits.StartTicks[i] = temp.fStartTick;
        hits.EndTicks[i] = temp.fEndTick;
        hits.PeakTimes[i] = temp.fPeakTime;
        hits.SigmaPeakTimes[i] = temp.fSigmaPeakTime;
        hits.RMSs[i] = temp.fRMS;
        hits.PeakAmplitudes[i] = temp.fPeakAmplitude;
        hits.SigmaPeakAmplitudes[i] = temp.fSigmaPeakAmplitude;
        hits.ROISummedADCs[i] = temp.fROISummedADC;
        hits.HitSummedADCs[i] = temp.fHitSummedADC;
        hits.Integrals[i] = temp.fIntegral;
        hits.SigmaIntegrals[i] = temp.fSigmaIntegral;
        hits.Multiplicities[i] = temp.fMultiplicity;
        hits.LocalIndices[i] = temp.fLocalIndex;
        hits.GoodnessOfFits[i] = temp.fGoodnessOfFit;
        hits.NDFs[i] = temp.fNDF;
        hits.SignalTypes[i] = temp.fSignalType;
        hits.WireID_Cryostats[i] = temp.fWireID_Cryostat;
        hits.WireID_TPCs[i] = temp.fWireID_TPC;
        hits.WireID_Planes[i] = temp.fWireID_Plane;
        hits.WireID_Wires[i] = temp.fWireID_Wire;
    }
    return hits;
}

SOAWireVector generateSOAEventWires(long long eventID, int numWires, int roisPerWire, std::mt19937& rng) {
    SOAWireVector wires;
    wires.EventIDs.resize(numWires, eventID);
    wires.Channels.resize(numWires);
    wires.Views.resize(numWires);
    wires.ROIs.resize(numWires);
    for (int i = 0; i < numWires; ++i) {
        WireIndividual temp = generateRandomWireIndividual(eventID, roisPerWire, rng);
        wires.Channels[i] = temp.fWire_Channel;
        wires.Views[i] = temp.fWire_View;
        wires.ROIs[i].resize(roisPerWire);
        for (int j = 0; j < roisPerWire; ++j) {
            wires.ROIs[i][j].data = temp.getSignalROI()[j].data;
        }
    }
    return wires;
}

std::vector<SOAROI> flattenSOAROIs(const SOAWireVector& wires) {
    std::vector<SOAROI> flat;
    for (const auto& wireROIs : wires.ROIs) {
        for (const auto& roi : wireROIs) {
            flat.push_back(roi);
        }
    }
    return flat;
}

std::vector<SOAWireBase> extractSOABaseWires(const SOAWireVector& wires) {
    std::vector<SOAWireBase> base(wires.EventIDs.size());
    for (size_t i = 0; i < wires.EventIDs.size(); ++i) {
        base[i].EventID = wires.EventIDs[i];
        base[i].Channel = wires.Channels[i];
        base[i].View = wires.Views[i];
    }
    return base;
}

// SOA Model Creators
auto CreateSOAAllDataProductModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<EventSOA>("EventSOA");
    return {std::move(model), model->GetToken("EventSOA")};
}

auto CreateSOAHitsModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<SOAHitVector>("hits");
    return {std::move(model), model->GetToken("hits")};
}

auto CreateSOAWiresModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<SOAWireVector>("wires");
    return {std::move(model), model->GetToken("wires")};
}

auto CreateSOAROIsModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<std::vector<SOAROI>>("rois");
    return {std::move(model), model->GetToken("rois")};
}

auto CreateSOABaseWiresModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<std::vector<SOAWireBase>>("wires");
    return {std::move(model), model->GetToken("wires")};
}

// SOA Work Functions
double RunSOA_event_allDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry, ROOT::RFieldToken token, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        EventSOA eventData;
        // Deterministic generation independent of thread/config
        // Build SOA vectors from deterministic AOS individuals to avoid duplication
        auto aosHits = generateEventHitsDeterministic(evt, hitsPerEvent);
        SOAHitVector soaHits;
        soaHits.EventIDs.resize(hitsPerEvent, evt);
        soaHits.Channels.resize(hitsPerEvent);
        soaHits.Views.resize(hitsPerEvent);
        soaHits.StartTicks.resize(hitsPerEvent);
        soaHits.EndTicks.resize(hitsPerEvent);
        soaHits.PeakTimes.resize(hitsPerEvent);
        soaHits.SigmaPeakTimes.resize(hitsPerEvent);
        soaHits.RMSs.resize(hitsPerEvent);
        soaHits.PeakAmplitudes.resize(hitsPerEvent);
        soaHits.SigmaPeakAmplitudes.resize(hitsPerEvent);
        soaHits.ROISummedADCs.resize(hitsPerEvent);
        soaHits.HitSummedADCs.resize(hitsPerEvent);
        soaHits.Integrals.resize(hitsPerEvent);
        soaHits.SigmaIntegrals.resize(hitsPerEvent);
        soaHits.Multiplicities.resize(hitsPerEvent);
        soaHits.LocalIndices.resize(hitsPerEvent);
        soaHits.GoodnessOfFits.resize(hitsPerEvent);
        soaHits.NDFs.resize(hitsPerEvent);
        soaHits.SignalTypes.resize(hitsPerEvent);
        soaHits.WireID_Cryostats.resize(hitsPerEvent);
        soaHits.WireID_TPCs.resize(hitsPerEvent);
        soaHits.WireID_Planes.resize(hitsPerEvent);
        soaHits.WireID_Wires.resize(hitsPerEvent);
        for (int i = 0; i < hitsPerEvent; ++i) {
            const auto& h = aosHits[i];
            soaHits.Channels[i] = h.fChannel;
            soaHits.Views[i] = h.fView;
            soaHits.StartTicks[i] = h.fStartTick;
            soaHits.EndTicks[i] = h.fEndTick;
            soaHits.PeakTimes[i] = h.fPeakTime;
            soaHits.SigmaPeakTimes[i] = h.fSigmaPeakTime;
            soaHits.RMSs[i] = h.fRMS;
            soaHits.PeakAmplitudes[i] = h.fPeakAmplitude;
            soaHits.SigmaPeakAmplitudes[i] = h.fSigmaPeakAmplitude;
            soaHits.ROISummedADCs[i] = h.fROISummedADC;
            soaHits.HitSummedADCs[i] = h.fHitSummedADC;
            soaHits.Integrals[i] = h.fIntegral;
            soaHits.SigmaIntegrals[i] = h.fSigmaIntegral;
            soaHits.Multiplicities[i] = h.fMultiplicity;
            soaHits.LocalIndices[i] = h.fLocalIndex;
            soaHits.GoodnessOfFits[i] = h.fGoodnessOfFit;
            soaHits.NDFs[i] = h.fNDF;
            soaHits.SignalTypes[i] = h.fSignalType;
            soaHits.WireID_Cryostats[i] = h.fWireID_Cryostat;
            soaHits.WireID_TPCs[i] = h.fWireID_TPC;
            soaHits.WireID_Planes[i] = h.fWireID_Plane;
            soaHits.WireID_Wires[i] = h.fWireID_Wire;
        }

        auto aosWires = generateEventWiresDeterministic(evt, wiresPerEvent, roisPerWire);
        SOAWireVector soaWires;
        soaWires.EventIDs.resize(wiresPerEvent, evt);
        soaWires.Channels.resize(wiresPerEvent);
        soaWires.Views.resize(wiresPerEvent);
        soaWires.ROIs.resize(wiresPerEvent);
        for (int w = 0; w < wiresPerEvent; ++w) {
            const auto& wi = aosWires[w];
            soaWires.Channels[w] = wi.fWire_Channel;
            soaWires.Views[w] = wi.fWire_View;
            soaWires.ROIs[w].resize(static_cast<int>(wi.getSignalROI().size()));
            for (size_t r = 0; r < wi.getSignalROI().size(); ++r) {
                soaWires.ROIs[w][r].data = wi.getSignalROI()[r].data;
            }
        }

        eventData.hits = std::move(soaHits);
        eventData.wires = std::move(soaWires);
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

double RunSOA_event_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        // Deterministic generation by building SOA from deterministic AOS individuals
        auto aosHits = generateEventHitsDeterministic(evt, hitsPerEvent);
        SOAHitVector hits;
        hits.EventIDs.resize(hitsPerEvent, evt);
        hits.Channels.resize(hitsPerEvent);
        hits.Views.resize(hitsPerEvent);
        hits.StartTicks.resize(hitsPerEvent);
        hits.EndTicks.resize(hitsPerEvent);
        hits.PeakTimes.resize(hitsPerEvent);
        hits.SigmaPeakTimes.resize(hitsPerEvent);
        hits.RMSs.resize(hitsPerEvent);
        hits.PeakAmplitudes.resize(hitsPerEvent);
        hits.SigmaPeakAmplitudes.resize(hitsPerEvent);
        hits.ROISummedADCs.resize(hitsPerEvent);
        hits.HitSummedADCs.resize(hitsPerEvent);
        hits.Integrals.resize(hitsPerEvent);
        hits.SigmaIntegrals.resize(hitsPerEvent);
        hits.Multiplicities.resize(hitsPerEvent);
        hits.LocalIndices.resize(hitsPerEvent);
        hits.GoodnessOfFits.resize(hitsPerEvent);
        hits.NDFs.resize(hitsPerEvent);
        hits.SignalTypes.resize(hitsPerEvent);
        hits.WireID_Cryostats.resize(hitsPerEvent);
        hits.WireID_TPCs.resize(hitsPerEvent);
        hits.WireID_Planes.resize(hitsPerEvent);
        hits.WireID_Wires.resize(hitsPerEvent);
        for (int i = 0; i < hitsPerEvent; ++i) {
            const auto& h = aosHits[i];
            hits.Channels[i] = h.fChannel;
            hits.Views[i] = h.fView;
            hits.StartTicks[i] = h.fStartTick;
            hits.EndTicks[i] = h.fEndTick;
            hits.PeakTimes[i] = h.fPeakTime;
            hits.SigmaPeakTimes[i] = h.fSigmaPeakTime;
            hits.RMSs[i] = h.fRMS;
            hits.PeakAmplitudes[i] = h.fPeakAmplitude;
            hits.SigmaPeakAmplitudes[i] = h.fSigmaPeakAmplitude;
            hits.ROISummedADCs[i] = h.fROISummedADC;
            hits.HitSummedADCs[i] = h.fHitSummedADC;
            hits.Integrals[i] = h.fIntegral;
            hits.SigmaIntegrals[i] = h.fSigmaIntegral;
            hits.Multiplicities[i] = h.fMultiplicity;
            hits.LocalIndices[i] = h.fLocalIndex;
            hits.GoodnessOfFits[i] = h.fGoodnessOfFit;
            hits.NDFs[i] = h.fNDF;
            hits.SignalTypes[i] = h.fSignalType;
            hits.WireID_Cryostats[i] = h.fWireID_Cryostat;
            hits.WireID_TPCs[i] = h.fWireID_TPC;
            hits.WireID_Planes[i] = h.fWireID_Plane;
            hits.WireID_Wires[i] = h.fWireID_Wire;
        }

        auto aosWires = generateEventWiresDeterministic(evt, wiresPerEvent, roisPerWire);
        SOAWireVector wires;
        wires.EventIDs.resize(wiresPerEvent, evt);
        wires.Channels.resize(wiresPerEvent);
        wires.Views.resize(wiresPerEvent);
        wires.ROIs.resize(wiresPerEvent);
        for (int w = 0; w < wiresPerEvent; ++w) {
            const auto& wi = aosWires[w];
            wires.Channels[w] = wi.fWire_Channel;
            wires.Views[w] = wi.fWire_View;
            wires.ROIs[w].resize(static_cast<int>(wi.getSignalROI().size()));
            for (size_t r = 0; r < wi.getSignalROI().size(); ++r) {
                wires.ROIs[w][r].data = wi.getSignalROI()[r].data;
            }
        }
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

double RunSOA_event_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int hitsPerEvent, int wiresPerEvent, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;
    for (int evt = first; evt < last; ++evt) {
        // Deterministic generation by building SOA from deterministic AOS individuals
        auto aosHits = generateEventHitsDeterministic(evt, hitsPerEvent);
        SOAHitVector hits;
        hits.EventIDs.resize(hitsPerEvent, evt);
        hits.Channels.resize(hitsPerEvent);
        hits.Views.resize(hitsPerEvent);
        hits.StartTicks.resize(hitsPerEvent);
        hits.EndTicks.resize(hitsPerEvent);
        hits.PeakTimes.resize(hitsPerEvent);
        hits.SigmaPeakTimes.resize(hitsPerEvent);
        hits.RMSs.resize(hitsPerEvent);
        hits.PeakAmplitudes.resize(hitsPerEvent);
        hits.SigmaPeakAmplitudes.resize(hitsPerEvent);
        hits.ROISummedADCs.resize(hitsPerEvent);
        hits.HitSummedADCs.resize(hitsPerEvent);
        hits.Integrals.resize(hitsPerEvent);
        hits.SigmaIntegrals.resize(hitsPerEvent);
        hits.Multiplicities.resize(hitsPerEvent);
        hits.LocalIndices.resize(hitsPerEvent);
        hits.GoodnessOfFits.resize(hitsPerEvent);
        hits.NDFs.resize(hitsPerEvent);
        hits.SignalTypes.resize(hitsPerEvent);
        hits.WireID_Cryostats.resize(hitsPerEvent);
        hits.WireID_TPCs.resize(hitsPerEvent);
        hits.WireID_Planes.resize(hitsPerEvent);
        hits.WireID_Wires.resize(hitsPerEvent);
        for (int i = 0; i < hitsPerEvent; ++i) {
            const auto& h = aosHits[i];
            hits.Channels[i] = h.fChannel;
            hits.Views[i] = h.fView;
            hits.StartTicks[i] = h.fStartTick;
            hits.EndTicks[i] = h.fEndTick;
            hits.PeakTimes[i] = h.fPeakTime;
            hits.SigmaPeakTimes[i] = h.fSigmaPeakTime;
            hits.RMSs[i] = h.fRMS;
            hits.PeakAmplitudes[i] = h.fPeakAmplitude;
            hits.SigmaPeakAmplitudes[i] = h.fSigmaPeakAmplitude;
            hits.ROISummedADCs[i] = h.fROISummedADC;
            hits.HitSummedADCs[i] = h.fHitSummedADC;
            hits.Integrals[i] = h.fIntegral;
            hits.SigmaIntegrals[i] = h.fSigmaIntegral;
            hits.Multiplicities[i] = h.fMultiplicity;
            hits.LocalIndices[i] = h.fLocalIndex;
            hits.GoodnessOfFits[i] = h.fGoodnessOfFit;
            hits.NDFs[i] = h.fNDF;
            hits.SignalTypes[i] = h.fSignalType;
            hits.WireID_Cryostats[i] = h.fWireID_Cryostat;
            hits.WireID_TPCs[i] = h.fWireID_TPC;
            hits.WireID_Planes[i] = h.fWireID_Plane;
            hits.WireID_Wires[i] = h.fWireID_Wire;
        }

        auto aosWires = generateEventWiresDeterministic(evt, wiresPerEvent, roisPerWire);
        SOAWireVector wires;
        wires.EventIDs.resize(wiresPerEvent, evt);
        wires.Channels.resize(wiresPerEvent);
        wires.Views.resize(wiresPerEvent);
        wires.ROIs.resize(wiresPerEvent);
        for (int w = 0; w < wiresPerEvent; ++w) {
            const auto& wi = aosWires[w];
            wires.Channels[w] = wi.fWire_Channel;
            wires.Views[w] = wi.fWire_View;
            wires.ROIs[w].resize(static_cast<int>(wi.getSignalROI().size()));
            for (size_t r = 0; r < wi.getSignalROI().size(); ++r) {
                wires.ROIs[w][r].data = wi.getSignalROI()[r].data;
            }
        }
        auto rois = flattenSOAROIs(wires);
        auto baseWires = extractSOABaseWires(wires);
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

// Adjusted for spills (reuse with adjusted counts)
SOAHitVector generateSOASpillHits(long long spillID, int adjustedHits, std::mt19937& rng) {
    return generateSOAEventHits(spillID, adjustedHits, rng);  // Reuse, but with spillID
}

SOAWireVector generateSOASpillWires(long long spillID, int adjustedWires, int roisPerWire, std::mt19937& rng) {
    return generateSOAEventWires(spillID, adjustedWires, roisPerWire, rng);
}

// Single generation for topObject/element
SOAHit generateSOASingleHit(long long id, std::mt19937& rng) {
    HitIndividual temp = generateRandomHitIndividual(id, rng);
    SOAHit hit;
    hit.EventID = temp.EventID;
    hit.Channel = temp.fChannel;
    hit.View = temp.fView;
    hit.StartTick = temp.fStartTick;
    hit.EndTick = temp.fEndTick;
    hit.PeakTime = temp.fPeakTime;
    hit.SigmaPeakTime = temp.fSigmaPeakTime;
    hit.RMS = temp.fRMS;
    hit.PeakAmplitude = temp.fPeakAmplitude;
    hit.SigmaPeakAmplitude = temp.fSigmaPeakAmplitude;
    hit.ROISummedADC = temp.fROISummedADC;
    hit.HitSummedADC = temp.fHitSummedADC;
    hit.Integral = temp.fIntegral;
    hit.SigmaIntegral = temp.fSigmaIntegral;
    hit.Multiplicity = temp.fMultiplicity;
    hit.LocalIndex = temp.fLocalIndex;
    hit.GoodnessOfFit = temp.fGoodnessOfFit;
    hit.NDF = temp.fNDF;
    hit.SignalType = temp.fSignalType;
    hit.WireID_Cryostat = temp.fWireID_Cryostat;
    hit.WireID_TPC = temp.fWireID_TPC;
    hit.WireID_Plane = temp.fWireID_Plane;
    hit.WireID_Wire = temp.fWireID_Wire;
    return hit;
}

SOAWire generateSOASingleWire(long long id, int roisPerWire, std::mt19937& rng) {
    WireIndividual temp = generateRandomWireIndividual(id, roisPerWire, rng);
    SOAWire wire;
    wire.EventID = temp.EventID;
    wire.Channel = temp.fWire_Channel;
    wire.View = temp.fWire_View;
    wire.ROIs.resize(roisPerWire);
    for (int j = 0; j < roisPerWire; ++j) {
        wire.ROIs[j].data = temp.getSignalROI()[j].data;
    }
    return wire;
}

FlatSOAROI generateSOASingleROI(unsigned int eventID, unsigned int wireID, std::mt19937& rng) {
    FlatSOAROI roi;
    std::uniform_real_distribution<float> distADC(0.0f, 100.0f);
    roi.EventID = eventID;
    roi.WireID  = wireID;
    roi.data.resize(10);
    for (auto& val : roi.data) val = distADC(rng);
    return roi;
}

std::vector<FlatSOAROI> flattenSOAROIsWithID(const SOAWireVector& wires) {
    std::vector<FlatSOAROI> flat;
    for (size_t w = 0; w < wires.Channels.size(); ++w) {
        for (const auto& roi : wires.ROIs[w]) {
            FlatSOAROI froi;
            froi.EventID = wires.EventIDs[w];
            froi.WireID  = wires.Channels[w];
            froi.data    = roi.data;
            flat.push_back(froi);
        }
    }
    return flat;
}

// SOA spill work functions (mirror AOS but use SOA gen)
double RunSOA_spill_allDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::Experimental::Detail::RRawPtrWriteEntry& entry, ROOT::RFieldToken token, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numSpills;
        int spill = idx % numSpills;
        int startHit = spill * adjustedHits;
        int startWire = spill * adjustedWires;
        EventSOA spillData;
        // Build deterministic SOA from deterministic AOS slices
        auto aosHits = generateEventHitsDeterministicRange(evt, startHit, adjustedHits);
        SOAHitVector hits;
        hits.EventIDs.resize(adjustedHits, evt);
        hits.Channels.resize(adjustedHits);
        hits.Views.resize(adjustedHits);
        hits.StartTicks.resize(adjustedHits);
        hits.EndTicks.resize(adjustedHits);
        hits.PeakTimes.resize(adjustedHits);
        hits.SigmaPeakTimes.resize(adjustedHits);
        hits.RMSs.resize(adjustedHits);
        hits.PeakAmplitudes.resize(adjustedHits);
        hits.SigmaPeakAmplitudes.resize(adjustedHits);
        hits.ROISummedADCs.resize(adjustedHits);
        hits.HitSummedADCs.resize(adjustedHits);
        hits.Integrals.resize(adjustedHits);
        hits.SigmaIntegrals.resize(adjustedHits);
        hits.Multiplicities.resize(adjustedHits);
        hits.LocalIndices.resize(adjustedHits);
        hits.GoodnessOfFits.resize(adjustedHits);
        hits.NDFs.resize(adjustedHits);
        hits.SignalTypes.resize(adjustedHits);
        hits.WireID_Cryostats.resize(adjustedHits);
        hits.WireID_TPCs.resize(adjustedHits);
        hits.WireID_Planes.resize(adjustedHits);
        hits.WireID_Wires.resize(adjustedHits);
        for (int i = 0; i < adjustedHits; ++i) {
            const auto& h = aosHits[i];
            hits.Channels[i] = h.fChannel;
            hits.Views[i] = h.fView;
            hits.StartTicks[i] = h.fStartTick;
            hits.EndTicks[i] = h.fEndTick;
            hits.PeakTimes[i] = h.fPeakTime;
            hits.SigmaPeakTimes[i] = h.fSigmaPeakTime;
            hits.RMSs[i] = h.fRMS;
            hits.PeakAmplitudes[i] = h.fPeakAmplitude;
            hits.SigmaPeakAmplitudes[i] = h.fSigmaPeakAmplitude;
            hits.ROISummedADCs[i] = h.fROISummedADC;
            hits.HitSummedADCs[i] = h.fHitSummedADC;
            hits.Integrals[i] = h.fIntegral;
            hits.SigmaIntegrals[i] = h.fSigmaIntegral;
            hits.Multiplicities[i] = h.fMultiplicity;
            hits.LocalIndices[i] = h.fLocalIndex;
            hits.GoodnessOfFits[i] = h.fGoodnessOfFit;
            hits.NDFs[i] = h.fNDF;
            hits.SignalTypes[i] = h.fSignalType;
            hits.WireID_Cryostats[i] = h.fWireID_Cryostat;
            hits.WireID_TPCs[i] = h.fWireID_TPC;
            hits.WireID_Planes[i] = h.fWireID_Plane;
            hits.WireID_Wires[i] = h.fWireID_Wire;
        }

        auto aosWires = generateEventWiresDeterministicRange(evt, startWire, adjustedWires, roisPerWire);
        SOAWireVector wires;
        wires.EventIDs.resize(adjustedWires, evt);
        wires.Channels.resize(adjustedWires);
        wires.Views.resize(adjustedWires);
        wires.ROIs.resize(adjustedWires);
        for (int w = 0; w < adjustedWires; ++w) {
            const auto& wi = aosWires[w];
            wires.Channels[w] = wi.fWire_Channel;
            wires.Views[w] = wi.fWire_View;
            wires.ROIs[w].resize(static_cast<int>(wi.getSignalROI().size()));
            for (size_t r = 0; r < wi.getSignalROI().size(); ++r) {
                wires.ROIs[w][r].data = wi.getSignalROI()[r].data;
            }
        }
        spillData.hits = std::move(hits);
        spillData.wires = std::move(wires);
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

double RunSOA_spill_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numSpills;
        int spill = idx % numSpills;
        int startHit = spill * adjustedHits;
        int startWire = spill * adjustedWires;

        // Build SOA from deterministic AOS slices
        auto aosHits = generateEventHitsDeterministicRange(evt, startHit, adjustedHits);
        SOAHitVector hits;
        hits.EventIDs.resize(adjustedHits, evt);
        hits.Channels.resize(adjustedHits);
        hits.Views.resize(adjustedHits);
        hits.StartTicks.resize(adjustedHits);
        hits.EndTicks.resize(adjustedHits);
        hits.PeakTimes.resize(adjustedHits);
        hits.SigmaPeakTimes.resize(adjustedHits);
        hits.RMSs.resize(adjustedHits);
        hits.PeakAmplitudes.resize(adjustedHits);
        hits.SigmaPeakAmplitudes.resize(adjustedHits);
        hits.ROISummedADCs.resize(adjustedHits);
        hits.HitSummedADCs.resize(adjustedHits);
        hits.Integrals.resize(adjustedHits);
        hits.SigmaIntegrals.resize(adjustedHits);
        hits.Multiplicities.resize(adjustedHits);
        hits.LocalIndices.resize(adjustedHits);
        hits.GoodnessOfFits.resize(adjustedHits);
        hits.NDFs.resize(adjustedHits);
        hits.SignalTypes.resize(adjustedHits);
        hits.WireID_Cryostats.resize(adjustedHits);
        hits.WireID_TPCs.resize(adjustedHits);
        hits.WireID_Planes.resize(adjustedHits);
        hits.WireID_Wires.resize(adjustedHits);
        for (int i = 0; i < adjustedHits; ++i) {
            const auto& h = aosHits[i];
            hits.Channels[i] = h.fChannel;
            hits.Views[i] = h.fView;
            hits.StartTicks[i] = h.fStartTick;
            hits.EndTicks[i] = h.fEndTick;
            hits.PeakTimes[i] = h.fPeakTime;
            hits.SigmaPeakTimes[i] = h.fSigmaPeakTime;
            hits.RMSs[i] = h.fRMS;
            hits.PeakAmplitudes[i] = h.fPeakAmplitude;
            hits.SigmaPeakAmplitudes[i] = h.fSigmaPeakAmplitude;
            hits.ROISummedADCs[i] = h.fROISummedADC;
            hits.HitSummedADCs[i] = h.fHitSummedADC;
            hits.Integrals[i] = h.fIntegral;
            hits.SigmaIntegrals[i] = h.fSigmaIntegral;
            hits.Multiplicities[i] = h.fMultiplicity;
            hits.LocalIndices[i] = h.fLocalIndex;
            hits.GoodnessOfFits[i] = h.fGoodnessOfFit;
            hits.NDFs[i] = h.fNDF;
            hits.SignalTypes[i] = h.fSignalType;
            hits.WireID_Cryostats[i] = h.fWireID_Cryostat;
            hits.WireID_TPCs[i] = h.fWireID_TPC;
            hits.WireID_Planes[i] = h.fWireID_Plane;
            hits.WireID_Wires[i] = h.fWireID_Wire;
        }

        auto aosWires = generateEventWiresDeterministicRange(evt, startWire, adjustedWires, roisPerWire);
        SOAWireVector wires;
        wires.EventIDs.resize(adjustedWires, evt);
        wires.Channels.resize(adjustedWires);
        wires.Views.resize(adjustedWires);
        wires.ROIs.resize(adjustedWires);
        for (int w = 0; w < adjustedWires; ++w) {
            const auto& wi = aosWires[w];
            wires.Channels[w] = wi.fWire_Channel;
            wires.Views[w] = wi.fWire_View;
            wires.ROIs[w].resize(static_cast<int>(wi.getSignalROI().size()));
            for (size_t r = 0; r < wi.getSignalROI().size(); ++r) {
                wires.ROIs[w][r].data = wi.getSignalROI()[r].data;
            }
        }
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

double RunSOA_spill_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& hitsEntry, ROOT::RFieldToken hitsToken, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& wiresEntry, ROOT::RFieldToken wiresToken, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::Experimental::Detail::RRawPtrWriteEntry& roisEntry, ROOT::RFieldToken roisToken, std::mutex& mutex, int numSpills, int adjustedHits, int adjustedWires, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numSpills;
        int spill = idx % numSpills;
        int startHit = spill * adjustedHits;
        int startWire = spill * adjustedWires;
        // Build SOA from deterministic AOS slices for consistency
        auto aosHits = generateEventHitsDeterministicRange(evt, startHit, adjustedHits);
        SOAHitVector hits;
        hits.EventIDs.resize(adjustedHits, evt);
        hits.Channels.resize(adjustedHits);
        hits.Views.resize(adjustedHits);
        hits.StartTicks.resize(adjustedHits);
        hits.EndTicks.resize(adjustedHits);
        hits.PeakTimes.resize(adjustedHits);
        hits.SigmaPeakTimes.resize(adjustedHits);
        hits.RMSs.resize(adjustedHits);
        hits.PeakAmplitudes.resize(adjustedHits);
        hits.SigmaPeakAmplitudes.resize(adjustedHits);
        hits.ROISummedADCs.resize(adjustedHits);
        hits.HitSummedADCs.resize(adjustedHits);
        hits.Integrals.resize(adjustedHits);
        hits.SigmaIntegrals.resize(adjustedHits);
        hits.Multiplicities.resize(adjustedHits);
        hits.LocalIndices.resize(adjustedHits);
        hits.GoodnessOfFits.resize(adjustedHits);
        hits.NDFs.resize(adjustedHits);
        hits.SignalTypes.resize(adjustedHits);
        hits.WireID_Cryostats.resize(adjustedHits);
        hits.WireID_TPCs.resize(adjustedHits);
        hits.WireID_Planes.resize(adjustedHits);
        hits.WireID_Wires.resize(adjustedHits);
        for (int i = 0; i < adjustedHits; ++i) {
            const auto& h = aosHits[i];
            hits.Channels[i] = h.fChannel;
            hits.Views[i] = h.fView;
            hits.StartTicks[i] = h.fStartTick;
            hits.EndTicks[i] = h.fEndTick;
            hits.PeakTimes[i] = h.fPeakTime;
            hits.SigmaPeakTimes[i] = h.fSigmaPeakTime;
            hits.RMSs[i] = h.fRMS;
            hits.PeakAmplitudes[i] = h.fPeakAmplitude;
            hits.SigmaPeakAmplitudes[i] = h.fSigmaPeakAmplitude;
            hits.ROISummedADCs[i] = h.fROISummedADC;
            hits.HitSummedADCs[i] = h.fHitSummedADC;
            hits.Integrals[i] = h.fIntegral;
            hits.SigmaIntegrals[i] = h.fSigmaIntegral;
            hits.Multiplicities[i] = h.fMultiplicity;
            hits.LocalIndices[i] = h.fLocalIndex;
            hits.GoodnessOfFits[i] = h.fGoodnessOfFit;
            hits.NDFs[i] = h.fNDF;
            hits.SignalTypes[i] = h.fSignalType;
            hits.WireID_Cryostats[i] = h.fWireID_Cryostat;
            hits.WireID_TPCs[i] = h.fWireID_TPC;
            hits.WireID_Planes[i] = h.fWireID_Plane;
            hits.WireID_Wires[i] = h.fWireID_Wire;
        }

        auto aosWires = generateEventWiresDeterministicRange(evt, startWire, adjustedWires, roisPerWire);
        SOAWireVector wires;
        wires.EventIDs.resize(adjustedWires, evt);
        wires.Channels.resize(adjustedWires);
        wires.Views.resize(adjustedWires);
        wires.ROIs.resize(adjustedWires);
        for (int w = 0; w < adjustedWires; ++w) {
            const auto& wi = aosWires[w];
            wires.Channels[w] = wi.fWire_Channel;
            wires.Views[w] = wi.fWire_View;
            wires.ROIs[w].resize(static_cast<int>(wi.getSignalROI().size()));
            for (size_t r = 0; r < wi.getSignalROI().size(); ++r) {
                wires.ROIs[w][r].data = wi.getSignalROI()[r].data;
            }
        }
        auto rois = flattenSOAROIs(wires);
        auto baseWires = extractSOABaseWires(wires);
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

// For topObject
SOAHit generateSOATopHit(long long id, std::mt19937& rng) {
    return generateSOASingleHit(id, rng);
}

SOAWire generateSOATopWire(long long id, int roisPerWire, std::mt19937& rng) {
    return generateSOASingleWire(id, roisPerWire, rng);
}

double RunSOA_topObject_perDataProductWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::REntry& wiresEntry, std::mutex& mutex, int roisPerWire) {
    TStopwatch sw;
    double totalTime = 0.0;
    auto hitPtr = hitsEntry.GetPtr<SOAHit>("hit");
    auto wirePtr = wiresEntry.GetPtr<SOAWire>("wire");
    for (int idx = first; idx < last; ++idx) {
        // Build SOA from deterministic AOS individuals
        std::uint32_t hSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('H'), static_cast<std::uint64_t>(idx), static_cast<std::uint64_t>(0));
        std::mt19937 hRng(hSeed);
        HitIndividual hInd = generateRandomHitIndividual(idx, hRng);
        SOAHit hit;
        hit.EventID = hInd.EventID;
        hit.Channel = hInd.fChannel;
        hit.View = hInd.fView;
        hit.StartTick = hInd.fStartTick;
        hit.EndTick = hInd.fEndTick;
        hit.PeakTime = hInd.fPeakTime;
        hit.SigmaPeakTime = hInd.fSigmaPeakTime;
        hit.RMS = hInd.fRMS;
        hit.PeakAmplitude = hInd.fPeakAmplitude;
        hit.SigmaPeakAmplitude = hInd.fSigmaPeakAmplitude;
        hit.ROISummedADC = hInd.fROISummedADC;
        hit.HitSummedADC = hInd.fHitSummedADC;
        hit.Integral = hInd.fIntegral;
        hit.SigmaIntegral = hInd.fSigmaIntegral;
        hit.Multiplicity = hInd.fMultiplicity;
        hit.LocalIndex = hInd.fLocalIndex;
        hit.GoodnessOfFit = hInd.fGoodnessOfFit;
        hit.NDF = hInd.fNDF;
        hit.SignalType = hInd.fSignalType;
        hit.WireID_Cryostat = hInd.fWireID_Cryostat;
        hit.WireID_TPC = hInd.fWireID_TPC;
        hit.WireID_Plane = hInd.fWireID_Plane;
        hit.WireID_Wire = hInd.fWireID_Wire;

        std::uint32_t wSeed = Utils::make_seed(Utils::kBaseSeed, static_cast<std::uint64_t>('W'), static_cast<std::uint64_t>(idx), static_cast<std::uint64_t>(0));
        std::mt19937 wRng(wSeed);
        WireIndividual wInd = generateRandomWireIndividual(idx, roisPerWire, wRng);
        SOAWire wire;
        wire.EventID = wInd.EventID;
        wire.Channel = wInd.fWire_Channel;
        wire.View = wInd.fWire_View;
        wire.ROIs.resize(wInd.getSignalROI().size());
        for (size_t r = 0; r < wInd.getSignalROI().size(); ++r) {
            wire.ROIs[r].data = wInd.getSignalROI()[r].data;
        }
        sw.Start();
        *hitPtr = hit;
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        *wirePtr = wire;
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    { std::lock_guard<std::mutex> lock(mutex);
      hitsContext.FlushCluster();
      wiresContext.FlushCluster(); }
    return totalTime;
}

// Add RunSOA_topObject_perGroupWorkFunc similarly, with flattening ROIs to vector<SOAROI> per row (but since per wire, it's the vector in SOAWire; for perGroup, separate base wire and flattened ROIs with WireID).

// For element
double RunSOA_element_hitsWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto hitPtr = entry.GetPtr<SOAHit>("hit");
    for (int idx = first; idx < last; ++idx) {
        *hitPtr = generateSOASingleHit(idx, rng);
        sw.Start();
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

// Similarly for wires (SOAWireBase), rois (FlatSOAROI or SOAROI with WireID), and wire_roi (SOAWire for perDataProduct).

// TopObject perGroup
double RunSOA_topObject_perGroupWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& hitsContext, ROOT::REntry& hitsEntry, ROOT::Experimental::RNTupleFillContext& wiresContext, ROOT::REntry& wiresEntry, ROOT::Experimental::RNTupleFillContext& roisContext, ROOT::REntry& roisEntry, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto hitPtr = hitsEntry.GetPtr<SOAHit>("hit");
    auto wirePtr = wiresEntry.GetPtr<SOAWireBase>("wire");
    auto roisPtr = roisEntry.GetPtr<std::vector<SOAROI>>("rois");
    for (int idx = first; idx < last; ++idx) {
        SOAHit hit = generateSOASingleHit(idx, rng);
        SOAWire fullWire = generateSOASingleWire(idx, roisPerWire, rng);
        SOAWireBase wireBase;
        wireBase.EventID = fullWire.EventID;
        wireBase.Channel = fullWire.Channel;
        wireBase.View = fullWire.View;
        auto rois = fullWire.ROIs;
        sw.Start();
        *hitPtr = hit;
        ROOT::RNTupleFillStatus hitsStatus;
        hitsContext.FillNoFlush(hitsEntry, hitsStatus);
        if (hitsStatus.ShouldFlushCluster()) {
            hitsContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitsContext.FlushCluster(); }
        }
        *wirePtr = wireBase;
        ROOT::RNTupleFillStatus wiresStatus;
        wiresContext.FillNoFlush(wiresEntry, wiresStatus);
        if (wiresStatus.ShouldFlushCluster()) {
            wiresContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wiresContext.FlushCluster(); }
        }
        *roisPtr = rois;
        ROOT::RNTupleFillStatus roisStatus;
        roisContext.FillNoFlush(roisEntry, roisStatus);
        if (roisStatus.ShouldFlushCluster()) {
            roisContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); roisContext.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    { std::lock_guard<std::mutex> lock(mutex);
      hitsContext.FlushCluster();
      wiresContext.FlushCluster();
      roisContext.FlushCluster(); }
    return totalTime;
}

// Element work funcs
double RunSOA_element_wireROIFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto wirePtr = entry.GetPtr<SOAWire>("wire_roi");
    for (int idx = first; idx < last; ++idx) {
        *wirePtr = generateSOASingleWire(idx, roisPerWire, rng);
        sw.Start();
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

double RunSOA_element_wiresWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto wirePtr = entry.GetPtr<SOAWireBase>("wire");
    for (int idx = first; idx < last; ++idx) {
        wirePtr->EventID = idx;
        wirePtr->Channel = rng() % 1024;
        wirePtr->View = rng() % 7;
        sw.Start();
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}

double RunSOA_element_roisWorkFunc(int first, int last, unsigned seed, ROOT::Experimental::RNTupleFillContext& context, ROOT::REntry& entry, std::mutex& mutex, int roisPerWire) {
    std::mt19937 rng(seed);
    TStopwatch sw;
    double totalTime = 0.0;
    auto roiPtr = entry.GetPtr<FlatSOAROI>("roi");
    for (int idx = first; idx < last; ++idx) {
        {
            unsigned int eventID = idx / (roisPerWire); // approximate mapping when standalone
            unsigned int wireIdx = idx % roisPerWire;   // assuming sequential order within event
            *roiPtr = generateSOASingleROI(eventID, wireIdx, rng);
        }
        sw.Start();
        ROOT::RNTupleFillStatus status;
        context.FillNoFlush(entry, status);
        if (status.ShouldFlushCluster()) {
            context.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); context.FlushCluster(); }
        }
        totalTime += sw.RealTime();
    }
    std::lock_guard<std::mutex> lock(mutex);
    context.FlushCluster();
    return totalTime;
}