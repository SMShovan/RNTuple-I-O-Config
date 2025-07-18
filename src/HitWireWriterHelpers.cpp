#include "HitWireWriterHelpers.hpp"
#include "HitWireGenerators.hpp" // For generateRandomHitVector, generateRandomWireVector
#include "Hit.hpp" // For HitVector
#include "Wire.hpp" // For WireVector
#include <ROOT/RNTupleFillStatus.hxx>
#include <TStopwatch.h>
#include <ROOT/RRawPtrWriteEntry.hxx>
#include <ROOT/RNTupleFillContext.hxx>
#include <ROOT/RField.hxx> // For base field types
#include <ROOT/RNTupleParallelWriter.hxx> // For parallel writer context
#include <ROOT/RNTupleWriteOptions.hxx> // If needed for options

using namespace ROOT::Detail;

// Implement hit model creation
auto CreateVertiSplitHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> {
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    ROOT::RFieldToken tEventID = hitModel->GetToken("EventID");
    auto ch = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    ROOT::RFieldToken tChannel = hitModel->GetToken("Channel");
    auto view = hitModel->MakeField<std::vector<int>>("View");
    ROOT::RFieldToken tView = hitModel->GetToken("View");
    auto sTick = hitModel->MakeField<std::vector<int>>("StartTick");
    ROOT::RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto eTick = hitModel->MakeField<std::vector<int>>("EndTick");
    ROOT::RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto peak = hitModel->MakeField<std::vector<float>>("PeakTime");
    ROOT::RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto sigmaPeak = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    ROOT::RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto rms = hitModel->MakeField<std::vector<float>>("RMS");
    ROOT::RFieldToken tRMS = hitModel->GetToken("RMS");
    auto amp = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    ROOT::RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    ROOT::RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    ROOT::RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto hitADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    ROOT::RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto integ = hitModel->MakeField<std::vector<float>>("Integral");
    ROOT::RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto sigmaInt = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    ROOT::RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto mult = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    ROOT::RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto locIdx = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    ROOT::RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto gof = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    ROOT::RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto ndf = hitModel->MakeField<std::vector<int>>("NDF");
    ROOT::RFieldToken tNDF = hitModel->GetToken("NDF");
    auto sigType = hitModel->MakeField<std::vector<int>>("SignalType");
    ROOT::RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto cryo = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    ROOT::RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto tpc = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    ROOT::RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    ROOT::RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto wire = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    ROOT::RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");

    std::unordered_map<std::string, ROOT::RFieldToken> tokens = {
        {"EventID", tEventID},
        {"Channel", tChannel},
        {"View", tView},
        {"StartTick", tStartTick},
        {"EndTick", tEndTick},
        {"PeakTime", tPeakTime},
        {"SigmaPeakTime", tSigmaPeakTime},
        {"RMS", tRMS},
        {"PeakAmplitude", tPeakAmplitude},
        {"SigmaPeakAmplitude", tSigmaPeakAmplitude},
        {"ROISummedADC", tROISummedADC},
        {"HitSummedADC", tHitSummedADC},
        {"Integral", tIntegral},
        {"SigmaIntegral", tSigmaIntegral},
        {"Multiplicity", tMultiplicity},
        {"LocalIndex", tLocalIndex},
        {"GoodnessOfFit", tGoodnessOfFit},
        {"NDF", tNDF},
        {"SignalType", tSignalType},
        {"WireID_Cryostat", tWireID_Cryostat},
        {"WireID_TPC", tWireID_TPC},
        {"WireID_Plane", tWireID_Plane},
        {"WireID_Wire", tWireID_Wire}
    };

    return {std::move(hitModel), tokens};
}

// Implement wire model creation
auto CreateWireModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<WireVector>("WireVector");
    ROOT::RFieldToken wireToken = wireModel->GetToken("WireVector");
    return {std::move(wireModel), wireToken};
}

// Implement work function
double RunVertiSplitWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads); // Approximate thread index

    TStopwatch sw;
    for (int evt = first; evt < last; ++evt) {
        HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
        WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, roisPerWire, rng);
        sw.Start();

        // Declare locals for binding
        long long localEventID = localHit.EventID;
        auto localChannel = std::move(localHit.getChannel());
        auto localView = std::move(localHit.getView());
        auto localStartTick = std::move(localHit.getStartTick());
        auto localEndTick = std::move(localHit.getEndTick());
        auto localPeakTime = std::move(localHit.getPeakTime());
        auto localSigmaPeakTime = std::move(localHit.getSigmaPeakTime());
        auto localRMS = std::move(localHit.getRMS());
        auto localPeakAmplitude = std::move(localHit.getPeakAmplitude());
        auto localSigmaPeakAmplitude = std::move(localHit.getSigmaPeakAmplitude());
        auto localROISummedADC = std::move(localHit.getROISummedADC());
        auto localHitSummedADC = std::move(localHit.getHitSummedADC());
        auto localIntegral = std::move(localHit.getIntegral());
        auto localSigmaIntegral = std::move(localHit.getSigmaIntegral());
        auto localMultiplicity = std::move(localHit.getMultiplicity());
        auto localLocalIndex = std::move(localHit.getLocalIndex());
        auto localGoodnessOfFit = std::move(localHit.getGoodnessOfFit());
        auto localNDF = std::move(localHit.getNDF());
        auto localSignalType = std::move(localHit.getSignalType());
        auto localWireID_Cryostat = std::move(localHit.getWireID_Cryostat());
        auto localWireID_TPC = std::move(localHit.getWireID_TPC());
        auto localWireID_Plane = std::move(localHit.getWireID_Plane());
        auto localWireID_Wire = std::move(localHit.getWireID_Wire());

        // Bind hit fields
        hitEntry.BindRawPtr(hitTokens.at("EventID"), &localEventID);
        hitEntry.BindRawPtr(hitTokens.at("Channel"), &localChannel);
        hitEntry.BindRawPtr(hitTokens.at("View"), &localView);
        hitEntry.BindRawPtr(hitTokens.at("StartTick"), &localStartTick);
        hitEntry.BindRawPtr(hitTokens.at("EndTick"), &localEndTick);
        hitEntry.BindRawPtr(hitTokens.at("PeakTime"), &localPeakTime);
        hitEntry.BindRawPtr(hitTokens.at("SigmaPeakTime"), &localSigmaPeakTime);
        hitEntry.BindRawPtr(hitTokens.at("RMS"), &localRMS);
        hitEntry.BindRawPtr(hitTokens.at("PeakAmplitude"), &localPeakAmplitude);
        hitEntry.BindRawPtr(hitTokens.at("SigmaPeakAmplitude"), &localSigmaPeakAmplitude);
        hitEntry.BindRawPtr(hitTokens.at("ROISummedADC"), &localROISummedADC);
        hitEntry.BindRawPtr(hitTokens.at("HitSummedADC"), &localHitSummedADC);
        hitEntry.BindRawPtr(hitTokens.at("Integral"), &localIntegral);
        hitEntry.BindRawPtr(hitTokens.at("SigmaIntegral"), &localSigmaIntegral);
        hitEntry.BindRawPtr(hitTokens.at("Multiplicity"), &localMultiplicity);
        hitEntry.BindRawPtr(hitTokens.at("LocalIndex"), &localLocalIndex);
        hitEntry.BindRawPtr(hitTokens.at("GoodnessOfFit"), &localGoodnessOfFit);
        hitEntry.BindRawPtr(hitTokens.at("NDF"), &localNDF);
        hitEntry.BindRawPtr(hitTokens.at("SignalType"), &localSignalType);
        hitEntry.BindRawPtr(hitTokens.at("WireID_Cryostat"), &localWireID_Cryostat);
        hitEntry.BindRawPtr(hitTokens.at("WireID_TPC"), &localWireID_TPC);
        hitEntry.BindRawPtr(hitTokens.at("WireID_Plane"), &localWireID_Plane);
        hitEntry.BindRawPtr(hitTokens.at("WireID_Wire"), &localWireID_Wire);
        ROOT::RNTupleFillStatus hitStatus;
        hitContext.FillNoFlush(hitEntry, hitStatus);
        if (hitStatus.ShouldFlushCluster()) {
            hitContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
        }

        // Bind and fill wires
        wireEntry.BindRawPtr(wireToken, &localWire);
        ROOT::RNTupleFillStatus wireStatus;
        wireContext.FillNoFlush(wireEntry, wireStatus);
        if (wireStatus.ShouldFlushCluster()) {
            wireContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
        }

        sw.Stop();
    }
    return sw.RealTime();
} 

// Implement HitVector model creation
auto CreateHitVectorModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<HitVector>("HitVector");
    ROOT::RFieldToken hitToken = hitModel->GetToken("HitVector");
    return {std::move(hitModel), hitToken};
}

// Implement WireVector model creation
auto CreateWireVectorModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<WireVector>("WireVector");
    ROOT::RFieldToken wireToken = wireModel->GetToken("WireVector");
    return {std::move(wireModel), wireToken};
}

// Implement work function
double RunHitWireVectorWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads);

    TStopwatch t;
    for (int i = first; i < last; ++i) {
        HitVector localHit = generateRandomHitVector(i, hitsPerEvent, rng);
        WireVector localWire = generateRandomWireVector(i, wiresPerEvent, roisPerWire, rng);

        t.Start();

        // Bind and fill hits
        hitEntry.BindRawPtr(hitToken, &localHit);
        ROOT::RNTupleFillStatus hitStatus;
        hitContext.FillNoFlush(hitEntry, hitStatus);
        if (hitStatus.ShouldFlushCluster()) {
            hitContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
        }

        // Bind and fill wires
        wireEntry.BindRawPtr(wireToken, &localWire);
        ROOT::RNTupleFillStatus wireStatus;
        wireContext.FillNoFlush(wireEntry, wireStatus);
        if (wireStatus.ShouldFlushCluster()) {
            wireContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
        }

        t.Stop();
    }
    return t.RealTime();
} 

// Implement HoriSpill hit model creation
auto CreateHoriSpillHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> {
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    ROOT::RFieldToken tEventID = hitModel->GetToken("EventID");
    auto spilID = hitModel->MakeField<int>("SpilID");
    ROOT::RFieldToken tSpilID = hitModel->GetToken("SpilID");
    auto ch = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    ROOT::RFieldToken tChannel = hitModel->GetToken("Channel");
    auto view = hitModel->MakeField<std::vector<int>>("View");
    ROOT::RFieldToken tView = hitModel->GetToken("View");
    auto sTick = hitModel->MakeField<std::vector<int>>("StartTick");
    ROOT::RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto eTick = hitModel->MakeField<std::vector<int>>("EndTick");
    ROOT::RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto peak = hitModel->MakeField<std::vector<float>>("PeakTime");
    ROOT::RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto sigmaPeak = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    ROOT::RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto rms = hitModel->MakeField<std::vector<float>>("RMS");
    ROOT::RFieldToken tRMS = hitModel->GetToken("RMS");
    auto amp = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    ROOT::RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    ROOT::RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    ROOT::RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto hitADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    ROOT::RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto integ = hitModel->MakeField<std::vector<float>>("Integral");
    ROOT::RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto sigmaInt = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    ROOT::RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto mult = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    ROOT::RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto locIdx = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    ROOT::RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto gof = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    ROOT::RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto ndf = hitModel->MakeField<std::vector<int>>("NDF");
    ROOT::RFieldToken tNDF = hitModel->GetToken("NDF");
    auto sigType = hitModel->MakeField<std::vector<int>>("SignalType");
    ROOT::RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto cryo = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    ROOT::RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto tpc = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    ROOT::RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    ROOT::RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto wire = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    ROOT::RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");

    std::unordered_map<std::string, ROOT::RFieldToken> tokens = {
        {"EventID", tEventID},
        {"SpilID", tSpilID},
        {"Channel", tChannel},
        {"View", tView},
        {"StartTick", tStartTick},
        {"EndTick", tEndTick},
        {"PeakTime", tPeakTime},
        {"SigmaPeakTime", tSigmaPeakTime},
        {"RMS", tRMS},
        {"PeakAmplitude", tPeakAmplitude},
        {"SigmaPeakAmplitude", tSigmaPeakAmplitude},
        {"ROISummedADC", tROISummedADC},
        {"HitSummedADC", tHitSummedADC},
        {"Integral", tIntegral},
        {"SigmaIntegral", tSigmaIntegral},
        {"Multiplicity", tMultiplicity},
        {"LocalIndex", tLocalIndex},
        {"GoodnessOfFit", tGoodnessOfFit},
        {"NDF", tNDF},
        {"SignalType", tSignalType},
        {"WireID_Cryostat", tWireID_Cryostat},
        {"WireID_TPC", tWireID_TPC},
        {"WireID_Plane", tWireID_Plane},
        {"WireID_Wire", tWireID_Wire}
    };
    return {std::move(hitModel), tokens};
}

// Implement work function
double RunHoriSpillWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (totalEntries / nThreads);

    TStopwatch sw;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numHoriSpills;
        int spil = idx % numHoriSpills;
        long long uid = static_cast<long long>(evt) * 10000 + spil;
        HitVector localHit = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
        WireVector localWire = generateRandomWireVector(uid, adjustedWiresPerEvent, roisPerWire, rng);
        sw.Start();

        // Declare locals
        long long localEventID = localHit.EventID;
        int localSpilID = spil;
        auto localChannel = std::move(localHit.getChannel());
        auto localView = std::move(localHit.getView());
        auto localStartTick = std::move(localHit.getStartTick());
        auto localEndTick = std::move(localHit.getEndTick());
        auto localPeakTime = std::move(localHit.getPeakTime());
        auto localSigmaPeakTime = std::move(localHit.getSigmaPeakTime());
        auto localRMS = std::move(localHit.getRMS());
        auto localPeakAmplitude = std::move(localHit.getPeakAmplitude());
        auto localSigmaPeakAmplitude = std::move(localHit.getSigmaPeakAmplitude());
        auto localROISummedADC = std::move(localHit.getROISummedADC());
        auto localHitSummedADC = std::move(localHit.getHitSummedADC());
        auto localIntegral = std::move(localHit.getIntegral());
        auto localSigmaIntegral = std::move(localHit.getSigmaIntegral());
        auto localMultiplicity = std::move(localHit.getMultiplicity());
        auto localLocalIndex = std::move(localHit.getLocalIndex());
        auto localGoodnessOfFit = std::move(localHit.getGoodnessOfFit());
        auto localNDF = std::move(localHit.getNDF());
        auto localSignalType = std::move(localHit.getSignalType());
        auto localWireID_Cryostat = std::move(localHit.getWireID_Cryostat());
        auto localWireID_TPC = std::move(localHit.getWireID_TPC());
        auto localWireID_Plane = std::move(localHit.getWireID_Plane());
        auto localWireID_Wire = std::move(localHit.getWireID_Wire());

        // Bind hit fields
        hitEntry.BindRawPtr(hitTokens.at("EventID"), &localEventID);
        hitEntry.BindRawPtr(hitTokens.at("SpilID"), &localSpilID);
        hitEntry.BindRawPtr(hitTokens.at("Channel"), &localChannel);
        hitEntry.BindRawPtr(hitTokens.at("View"), &localView);
        hitEntry.BindRawPtr(hitTokens.at("StartTick"), &localStartTick);
        hitEntry.BindRawPtr(hitTokens.at("EndTick"), &localEndTick);
        hitEntry.BindRawPtr(hitTokens.at("PeakTime"), &localPeakTime);
        hitEntry.BindRawPtr(hitTokens.at("SigmaPeakTime"), &localSigmaPeakTime);
        hitEntry.BindRawPtr(hitTokens.at("RMS"), &localRMS);
        hitEntry.BindRawPtr(hitTokens.at("PeakAmplitude"), &localPeakAmplitude);
        hitEntry.BindRawPtr(hitTokens.at("SigmaPeakAmplitude"), &localSigmaPeakAmplitude);
        hitEntry.BindRawPtr(hitTokens.at("ROISummedADC"), &localROISummedADC);
        hitEntry.BindRawPtr(hitTokens.at("HitSummedADC"), &localHitSummedADC);
        hitEntry.BindRawPtr(hitTokens.at("Integral"), &localIntegral);
        hitEntry.BindRawPtr(hitTokens.at("SigmaIntegral"), &localSigmaIntegral);
        hitEntry.BindRawPtr(hitTokens.at("Multiplicity"), &localMultiplicity);
        hitEntry.BindRawPtr(hitTokens.at("LocalIndex"), &localLocalIndex);
        hitEntry.BindRawPtr(hitTokens.at("GoodnessOfFit"), &localGoodnessOfFit);
        hitEntry.BindRawPtr(hitTokens.at("NDF"), &localNDF);
        hitEntry.BindRawPtr(hitTokens.at("SignalType"), &localSignalType);
        hitEntry.BindRawPtr(hitTokens.at("WireID_Cryostat"), &localWireID_Cryostat);
        hitEntry.BindRawPtr(hitTokens.at("WireID_TPC"), &localWireID_TPC);
        hitEntry.BindRawPtr(hitTokens.at("WireID_Plane"), &localWireID_Plane);
        hitEntry.BindRawPtr(hitTokens.at("WireID_Wire"), &localWireID_Wire);
        ROOT::RNTupleFillStatus hitStatus;
        hitContext.FillNoFlush(hitEntry, hitStatus);
        if (hitStatus.ShouldFlushCluster()) {
            hitContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
        }

        // Bind and fill wires
        wireEntry.BindRawPtr(wireToken, &localWire);
        ROOT::RNTupleFillStatus wireStatus;
        wireContext.FillNoFlush(wireEntry, wireStatus);
        if (wireStatus.ShouldFlushCluster()) {
            wireContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
        }

        sw.Stop();
    }
    return sw.RealTime();
} 

// Implement individual hit model creation
auto CreateIndividualHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> {
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    ROOT::RFieldToken tEventID = hitModel->GetToken("EventID");
    auto fChannel = hitModel->MakeField<unsigned int>("Channel");
    ROOT::RFieldToken tChannel = hitModel->GetToken("Channel");
    auto view = hitModel->MakeField<int>("View");
    ROOT::RFieldToken tView = hitModel->GetToken("View");
    auto sTick = hitModel->MakeField<int>("StartTick");
    ROOT::RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto eTick = hitModel->MakeField<int>("EndTick");
    ROOT::RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto peak = hitModel->MakeField<float>("PeakTime");
    ROOT::RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto sigmaPeak = hitModel->MakeField<float>("SigmaPeakTime");
    ROOT::RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto rms = hitModel->MakeField<float>("RMS");
    ROOT::RFieldToken tRMS = hitModel->GetToken("RMS");
    auto amp = hitModel->MakeField<float>("PeakAmplitude");
    ROOT::RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<float>("SigmaPeakAmplitude");
    ROOT::RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<float>("ROISummedADC");
    ROOT::RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto hitADC = hitModel->MakeField<float>("HitSummedADC");
    ROOT::RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto integ = hitModel->MakeField<float>("Integral");
    ROOT::RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto sigmaInt = hitModel->MakeField<float>("SigmaIntegral");
    ROOT::RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto mult = hitModel->MakeField<short int>("Multiplicity");
    ROOT::RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto locIdx = hitModel->MakeField<short int>("LocalIndex");
    ROOT::RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto gof = hitModel->MakeField<float>("GoodnessOfFit");
    ROOT::RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto ndf = hitModel->MakeField<int>("NDF");
    ROOT::RFieldToken tNDF = hitModel->GetToken("NDF");
    auto sigType = hitModel->MakeField<int>("SignalType");
    ROOT::RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto cryo = hitModel->MakeField<int>("WireID_Cryostat");
    ROOT::RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto tpc = hitModel->MakeField<int>("WireID_TPC");
    ROOT::RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto plane = hitModel->MakeField<int>("WireID_Plane");
    ROOT::RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto wire = hitModel->MakeField<int>("WireID_Wire");
    ROOT::RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");

    std::unordered_map<std::string, ROOT::RFieldToken> tokens = {
        {"EventID", tEventID},
        {"Channel", tChannel},
        {"View", tView},
        {"StartTick", tStartTick},
        {"EndTick", tEndTick},
        {"PeakTime", tPeakTime},
        {"SigmaPeakTime", tSigmaPeakTime},
        {"RMS", tRMS},
        {"PeakAmplitude", tPeakAmplitude},
        {"SigmaPeakAmplitude", tSigmaPeakAmplitude},
        {"ROISummedADC", tROISummedADC},
        {"HitSummedADC", tHitSummedADC},
        {"Integral", tIntegral},
        {"SigmaIntegral", tSigmaIntegral},
        {"Multiplicity", tMultiplicity},
        {"LocalIndex", tLocalIndex},
        {"GoodnessOfFit", tGoodnessOfFit},
        {"NDF", tNDF},
        {"SignalType", tSignalType},
        {"WireID_Cryostat", tWireID_Cryostat},
        {"WireID_TPC", tWireID_TPC},
        {"WireID_Plane", tWireID_Plane},
        {"WireID_Wire", tWireID_Wire}
    };
    return {std::move(hitModel), tokens};
}

// Implement individual wire model creation
auto CreateIndividualWireModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<WireIndividual>("WireIndividual");
    ROOT::RFieldToken wireToken = wireModel->GetToken("WireIndividual");
    return {std::move(wireModel), wireToken};
}

// Implement work function
double RunIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads);

    TStopwatch storageTimer;
    for (int eventIndex = first; eventIndex < last; ++eventIndex) {
        // Hits loop
        for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
            HitIndividual localHit = generateRandomHitIndividual(eventIndex, rng);
            storageTimer.Start();
            // Bind hit fields
            hitEntry.BindRawPtr(hitTokens.at("EventID"), &localHit.EventID);
            hitEntry.BindRawPtr(hitTokens.at("Channel"), &localHit.fChannel);
            hitEntry.BindRawPtr(hitTokens.at("View"), &localHit.fView);
            hitEntry.BindRawPtr(hitTokens.at("StartTick"), &localHit.fStartTick);
            hitEntry.BindRawPtr(hitTokens.at("EndTick"), &localHit.fEndTick);
            hitEntry.BindRawPtr(hitTokens.at("PeakTime"), &localHit.fPeakTime);
            hitEntry.BindRawPtr(hitTokens.at("SigmaPeakTime"), &localHit.fSigmaPeakTime);
            hitEntry.BindRawPtr(hitTokens.at("RMS"), &localHit.fRMS);
            hitEntry.BindRawPtr(hitTokens.at("PeakAmplitude"), &localHit.fPeakAmplitude);
            hitEntry.BindRawPtr(hitTokens.at("SigmaPeakAmplitude"), &localHit.fSigmaPeakAmplitude);
            hitEntry.BindRawPtr(hitTokens.at("ROISummedADC"), &localHit.fROISummedADC);
            hitEntry.BindRawPtr(hitTokens.at("HitSummedADC"), &localHit.fHitSummedADC);
            hitEntry.BindRawPtr(hitTokens.at("Integral"), &localHit.fIntegral);
            hitEntry.BindRawPtr(hitTokens.at("SigmaIntegral"), &localHit.fSigmaIntegral);
            hitEntry.BindRawPtr(hitTokens.at("Multiplicity"), &localHit.fMultiplicity);
            hitEntry.BindRawPtr(hitTokens.at("LocalIndex"), &localHit.fLocalIndex);
            hitEntry.BindRawPtr(hitTokens.at("GoodnessOfFit"), &localHit.fGoodnessOfFit);
            hitEntry.BindRawPtr(hitTokens.at("NDF"), &localHit.fNDF);
            hitEntry.BindRawPtr(hitTokens.at("SignalType"), &localHit.fSignalType);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Cryostat"), &localHit.fWireID_Cryostat);
            hitEntry.BindRawPtr(hitTokens.at("WireID_TPC"), &localHit.fWireID_TPC);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Plane"), &localHit.fWireID_Plane);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Wire"), &localHit.fWireID_Wire);
            ROOT::RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }
            storageTimer.Stop();
        }
        // Wires loop
        for (int wireIndex = 0; wireIndex < wiresPerEvent; ++wireIndex) {
            WireIndividual localWire = generateRandomWireIndividual(eventIndex, roisPerWire, rng);
            storageTimer.Start();
            wireEntry.BindRawPtr(wireToken, &localWire);
            ROOT::RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }
            storageTimer.Stop();
        }
    }
    return storageTimer.RealTime();
} 

// Implement VertiSplit individual hit model creation
auto CreateVertiSplitIndividualHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> {
    auto hitModel = ROOT::RNTupleModel::Create();
    auto fEventID = hitModel->MakeField<long long>("EventID");
    ROOT::RFieldToken tEventID = hitModel->GetToken("EventID");
    auto fChannel = hitModel->MakeField<unsigned int>("Channel");
    ROOT::RFieldToken tChannel = hitModel->GetToken("Channel");
    auto fView = hitModel->MakeField<int>("View");
    ROOT::RFieldToken tView = hitModel->GetToken("View");
    auto fStartTick = hitModel->MakeField<int>("StartTick");
    ROOT::RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto fEndTick = hitModel->MakeField<int>("EndTick");
    ROOT::RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto fPeakTime = hitModel->MakeField<float>("PeakTime");
    ROOT::RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto fSigmaPeakTime = hitModel->MakeField<float>("SigmaPeakTime");
    ROOT::RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto fRMS = hitModel->MakeField<float>("RMS");
    ROOT::RFieldToken tRMS = hitModel->GetToken("RMS");
    auto fPeakAmplitude = hitModel->MakeField<float>("PeakAmplitude");
    ROOT::RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto fSigmaPeakAmplitude = hitModel->MakeField<float>("SigmaPeakAmplitude");
    ROOT::RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto fROISummedADC = hitModel->MakeField<float>("ROISummedADC");
    ROOT::RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto fHitSummedADC = hitModel->MakeField<float>("HitSummedADC");
    ROOT::RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto fIntegral = hitModel->MakeField<float>("Integral");
    ROOT::RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto fSigmaIntegral = hitModel->MakeField<float>("SigmaIntegral");
    ROOT::RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto fMultiplicity = hitModel->MakeField<short int>("Multiplicity");
    ROOT::RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto fLocalIndex = hitModel->MakeField<short int>("LocalIndex");
    ROOT::RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto fGoodnessOfFit = hitModel->MakeField<float>("GoodnessOfFit");
    ROOT::RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto fNDF = hitModel->MakeField<int>("NDF");
    ROOT::RFieldToken tNDF = hitModel->GetToken("NDF");
    auto fSignalType = hitModel->MakeField<int>("SignalType");
    ROOT::RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto fWireID_Cryostat = hitModel->MakeField<int>("WireID_Cryostat");
    ROOT::RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto fWireID_TPC = hitModel->MakeField<int>("WireID_TPC");
    ROOT::RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto fWireID_Plane = hitModel->MakeField<int>("WireID_Plane");
    ROOT::RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto fWireID_Wire = hitModel->MakeField<int>("WireID_Wire");
    ROOT::RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");

    std::unordered_map<std::string, ROOT::RFieldToken> tokens = {
        {"EventID", tEventID},
        {"Channel", tChannel},
        {"View", tView},
        {"StartTick", tStartTick},
        {"EndTick", tEndTick},
        {"PeakTime", tPeakTime},
        {"SigmaPeakTime", tSigmaPeakTime},
        {"RMS", tRMS},
        {"PeakAmplitude", tPeakAmplitude},
        {"SigmaPeakAmplitude", tSigmaPeakAmplitude},
        {"ROISummedADC", tROISummedADC},
        {"HitSummedADC", tHitSummedADC},
        {"Integral", tIntegral},
        {"SigmaIntegral", tSigmaIntegral},
        {"Multiplicity", tMultiplicity},
        {"LocalIndex", tLocalIndex},
        {"GoodnessOfFit", tGoodnessOfFit},
        {"NDF", tNDF},
        {"SignalType", tSignalType},
        {"WireID_Cryostat", tWireID_Cryostat},
        {"WireID_TPC", tWireID_TPC},
        {"WireID_Plane", tWireID_Plane},
        {"WireID_Wire", tWireID_Wire}
    };
    return {std::move(hitModel), tokens};
}

// Implement work function
double RunVertiSplitIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads);

    TStopwatch sw;
    for (int evt = first; evt < last; ++evt) {
        // Hits loop
        for (int i = 0; i < hitsPerEvent; ++i) {
            HitIndividual localHit = generateRandomHitIndividual(evt, rng);
            sw.Start();
            // Bind and fill hit
            hitEntry.BindRawPtr(hitTokens.at("EventID"), &localHit.EventID);
            hitEntry.BindRawPtr(hitTokens.at("Channel"), &localHit.fChannel);
            hitEntry.BindRawPtr(hitTokens.at("View"), &localHit.fView);
            hitEntry.BindRawPtr(hitTokens.at("StartTick"), &localHit.fStartTick);
            hitEntry.BindRawPtr(hitTokens.at("EndTick"), &localHit.fEndTick);
            hitEntry.BindRawPtr(hitTokens.at("PeakTime"), &localHit.fPeakTime);
            hitEntry.BindRawPtr(hitTokens.at("SigmaPeakTime"), &localHit.fSigmaPeakTime);
            hitEntry.BindRawPtr(hitTokens.at("RMS"), &localHit.fRMS);
            hitEntry.BindRawPtr(hitTokens.at("PeakAmplitude"), &localHit.fPeakAmplitude);
            hitEntry.BindRawPtr(hitTokens.at("SigmaPeakAmplitude"), &localHit.fSigmaPeakAmplitude);
            hitEntry.BindRawPtr(hitTokens.at("ROISummedADC"), &localHit.fROISummedADC);
            hitEntry.BindRawPtr(hitTokens.at("HitSummedADC"), &localHit.fHitSummedADC);
            hitEntry.BindRawPtr(hitTokens.at("Integral"), &localHit.fIntegral);
            hitEntry.BindRawPtr(hitTokens.at("SigmaIntegral"), &localHit.fSigmaIntegral);
            hitEntry.BindRawPtr(hitTokens.at("Multiplicity"), &localHit.fMultiplicity);
            hitEntry.BindRawPtr(hitTokens.at("LocalIndex"), &localHit.fLocalIndex);
            hitEntry.BindRawPtr(hitTokens.at("GoodnessOfFit"), &localHit.fGoodnessOfFit);
            hitEntry.BindRawPtr(hitTokens.at("NDF"), &localHit.fNDF);
            hitEntry.BindRawPtr(hitTokens.at("SignalType"), &localHit.fSignalType);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Cryostat"), &localHit.fWireID_Cryostat);
            hitEntry.BindRawPtr(hitTokens.at("WireID_TPC"), &localHit.fWireID_TPC);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Plane"), &localHit.fWireID_Plane);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Wire"), &localHit.fWireID_Wire);
            ROOT::RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }
            sw.Stop();
        }
        // Wires loop
        for (int i = 0; i < wiresPerEvent; ++i) {
            WireIndividual localWire = generateRandomWireIndividual(evt, roisPerWire, rng);
            sw.Start();
            // Bind and fill wire
            wireEntry.BindRawPtr(wireToken, &localWire);
            ROOT::RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }
            sw.Stop();
        }
    }
    return sw.RealTime();
} 

// Implement HoriSpill individual hit model creation
auto CreateHoriSpillIndividualHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> {
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    ROOT::RFieldToken tEventID = hitModel->GetToken("EventID");
    auto spilID = hitModel->MakeField<int>("SpilID");
    ROOT::RFieldToken tSpilID = hitModel->GetToken("SpilID");
    auto ch = hitModel->MakeField<unsigned int>("Channel");
    ROOT::RFieldToken tChannel = hitModel->GetToken("Channel");
    auto view = hitModel->MakeField<int>("View");
    ROOT::RFieldToken tView = hitModel->GetToken("View");
    auto sTick = hitModel->MakeField<int>("StartTick");
    ROOT::RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto eTick = hitModel->MakeField<int>("EndTick");
    ROOT::RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto peak = hitModel->MakeField<float>("PeakTime");
    ROOT::RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto sigmaPeak = hitModel->MakeField<float>("SigmaPeakTime");
    ROOT::RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto rms = hitModel->MakeField<float>("RMS");
    ROOT::RFieldToken tRMS = hitModel->GetToken("RMS");
    auto amp = hitModel->MakeField<float>("PeakAmplitude");
    ROOT::RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<float>("SigmaPeakAmplitude");
    ROOT::RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<float>("ROISummedADC");
    ROOT::RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto hitADC = hitModel->MakeField<float>("HitSummedADC");
    ROOT::RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto integ = hitModel->MakeField<float>("Integral");
    ROOT::RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto sigmaInt = hitModel->MakeField<float>("SigmaIntegral");
    ROOT::RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto mult = hitModel->MakeField<short int>("Multiplicity");
    ROOT::RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto locIdx = hitModel->MakeField<short int>("LocalIndex");
    ROOT::RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto gof = hitModel->MakeField<float>("GoodnessOfFit");
    ROOT::RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto ndf = hitModel->MakeField<int>("NDF");
    ROOT::RFieldToken tNDF = hitModel->GetToken("NDF");
    auto sigType = hitModel->MakeField<int>("SignalType");
    ROOT::RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto cryo = hitModel->MakeField<int>("WireID_Cryostat");
    ROOT::RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto tpc = hitModel->MakeField<int>("WireID_TPC");
    ROOT::RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto plane = hitModel->MakeField<int>("WireID_Plane");
    ROOT::RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto wire = hitModel->MakeField<int>("WireID_Wire");
    ROOT::RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");

    std::unordered_map<std::string, ROOT::RFieldToken> tokens = {
        {"EventID", tEventID},
        {"SpilID", tSpilID},
        {"Channel", tChannel},
        {"View", tView},
        {"StartTick", tStartTick},
        {"EndTick", tEndTick},
        {"PeakTime", tPeakTime},
        {"SigmaPeakTime", tSigmaPeakTime},
        {"RMS", tRMS},
        {"PeakAmplitude", tPeakAmplitude},
        {"SigmaPeakAmplitude", tSigmaPeakAmplitude},
        {"ROISummedADC", tROISummedADC},
        {"HitSummedADC", tHitSummedADC},
        {"Integral", tIntegral},
        {"SigmaIntegral", tSigmaIntegral},
        {"Multiplicity", tMultiplicity},
        {"LocalIndex", tLocalIndex},
        {"GoodnessOfFit", tGoodnessOfFit},
        {"NDF", tNDF},
        {"SignalType", tSignalType},
        {"WireID_Cryostat", tWireID_Cryostat},
        {"WireID_TPC", tWireID_TPC},
        {"WireID_Plane", tWireID_Plane},
        {"WireID_Wire", tWireID_Wire}
    };
    return {std::move(hitModel), tokens};
}

// Implement work function
double RunHoriSpillIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (totalEntries / nThreads);

    TStopwatch sw;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numHoriSpills;
        int spil = idx % numHoriSpills;
        long long uid = static_cast<long long>(evt) * 10000 + spil;
        int localSpilID = spil;
        // Hits loop
        for (int h = 0; h < adjustedHitsPerEvent; ++h) {
            HitIndividual localHit = generateRandomHitIndividual(uid, rng);
            sw.Start();
            // Bind and fill hit
            hitEntry.BindRawPtr(hitTokens.at("EventID"), &localHit.EventID);
            hitEntry.BindRawPtr(hitTokens.at("SpilID"), &localSpilID);
            hitEntry.BindRawPtr(hitTokens.at("Channel"), &localHit.fChannel);
            hitEntry.BindRawPtr(hitTokens.at("View"), &localHit.fView);
            hitEntry.BindRawPtr(hitTokens.at("StartTick"), &localHit.fStartTick);
            hitEntry.BindRawPtr(hitTokens.at("EndTick"), &localHit.fEndTick);
            hitEntry.BindRawPtr(hitTokens.at("PeakTime"), &localHit.fPeakTime);
            hitEntry.BindRawPtr(hitTokens.at("SigmaPeakTime"), &localHit.fSigmaPeakTime);
            hitEntry.BindRawPtr(hitTokens.at("RMS"), &localHit.fRMS);
            hitEntry.BindRawPtr(hitTokens.at("PeakAmplitude"), &localHit.fPeakAmplitude);
            hitEntry.BindRawPtr(hitTokens.at("SigmaPeakAmplitude"), &localHit.fSigmaPeakAmplitude);
            hitEntry.BindRawPtr(hitTokens.at("ROISummedADC"), &localHit.fROISummedADC);
            hitEntry.BindRawPtr(hitTokens.at("HitSummedADC"), &localHit.fHitSummedADC);
            hitEntry.BindRawPtr(hitTokens.at("Integral"), &localHit.fIntegral);
            hitEntry.BindRawPtr(hitTokens.at("SigmaIntegral"), &localHit.fSigmaIntegral);
            hitEntry.BindRawPtr(hitTokens.at("Multiplicity"), &localHit.fMultiplicity);
            hitEntry.BindRawPtr(hitTokens.at("LocalIndex"), &localHit.fLocalIndex);
            hitEntry.BindRawPtr(hitTokens.at("GoodnessOfFit"), &localHit.fGoodnessOfFit);
            hitEntry.BindRawPtr(hitTokens.at("NDF"), &localHit.fNDF);
            hitEntry.BindRawPtr(hitTokens.at("SignalType"), &localHit.fSignalType);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Cryostat"), &localHit.fWireID_Cryostat);
            hitEntry.BindRawPtr(hitTokens.at("WireID_TPC"), &localHit.fWireID_TPC);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Plane"), &localHit.fWireID_Plane);
            hitEntry.BindRawPtr(hitTokens.at("WireID_Wire"), &localHit.fWireID_Wire);
            ROOT::RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }
            sw.Stop();
        }
        // Wires loop
        for (int w = 0; w < adjustedWiresPerEvent; ++w) {
            WireIndividual localWire = generateRandomWireIndividual(uid, roisPerWire, rng);
            sw.Start();
            // Bind and fill wire
            wireEntry.BindRawPtr(wireToken, &localWire);
            ROOT::RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }
            sw.Stop();
        }
    }
    return sw.RealTime();
} 

// Dict-specific model creation for individual hit
auto CreateDictIndividualHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<HitIndividual>("HitIndividual");
    ROOT::RFieldToken hitToken = hitModel->GetToken("HitIndividual");
    return {std::move(hitModel), hitToken};
}

// Dict-specific model creation for individual wire
auto CreateDictIndividualWireModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<WireIndividual>("WireIndividual");
    ROOT::RFieldToken wireToken = wireModel->GetToken("WireIndividual");
    return {std::move(wireModel), wireToken};
}

// Dict-specific work function for individual dict
double RunDictIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads);

    TStopwatch t;
    for (int evt = first; evt < last; ++evt) {
        // Hits loop
        for (int i = 0; i < hitsPerEvent; ++i) {
            HitIndividual localHit = generateRandomHitIndividual(evt, rng);
            t.Start();
            // Bind and fill hit
            hitEntry.BindRawPtr(hitToken, &localHit);
            ROOT::RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }
            t.Stop();
        }
        // Wires loop
        for (int i = 0; i < wiresPerEvent; ++i) {
            WireIndividual localWire = generateRandomWireIndividual(evt, roisPerWire, rng);
            t.Start();
            // Bind and fill wire
            wireEntry.BindRawPtr(wireToken, &localWire);
            ROOT::RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }
            t.Stop();
        }
    }
    return t.RealTime();
} 

// Dict-specific verti-split hit model and token (using single class field to match dict expectations)
auto CreateDictVertiSplitHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<HitVector>("HitVector");
    ROOT::RFieldToken hitToken = hitModel->GetToken("HitVector");
    return {std::move(hitModel), hitToken};
}

// Dict-specific work function for verti-split vector (binding single pointer)
double RunDictVertiSplitWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads);

    TStopwatch t;
    for (int evt = first; evt < last; ++evt) {
        HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
        WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, roisPerWire, rng);
        t.Start();

        // Bind and fill hits
        hitEntry.BindRawPtr(hitToken, &localHit);
        ROOT::RNTupleFillStatus hitStatus;
        hitContext.FillNoFlush(hitEntry, hitStatus);
        if (hitStatus.ShouldFlushCluster()) {
            hitContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
        }

        // Bind and fill wires
        wireEntry.BindRawPtr(wireToken, &localWire);
        ROOT::RNTupleFillStatus wireStatus;
        wireContext.FillNoFlush(wireEntry, wireStatus);
        if (wireStatus.ShouldFlushCluster()) {
            wireContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
        }

        t.Stop();
    }
    return t.RealTime();
} 

// Dict-specific verti-split individual hit model and token (single class field)
auto CreateDictVertiSplitIndividualHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<HitIndividual>("HitIndividual");
    ROOT::RFieldToken hitToken = hitModel->GetToken("HitIndividual");
    return {std::move(hitModel), hitToken};
}

// Dict-specific work function for verti-split individual
double RunDictVertiSplitIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads);

    TStopwatch t;
    for (int evt = first; evt < last; ++evt) {
        // Hits loop
        for (int i = 0; i < hitsPerEvent; ++i) {
            HitIndividual localHit = generateRandomHitIndividual(evt, rng);
            t.Start();
            hitEntry.BindRawPtr(hitToken, &localHit);
            ROOT::RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }
            t.Stop();
        }
        // Wires loop
        for (int i = 0; i < wiresPerEvent; ++i) {
            WireIndividual localWire = generateRandomWireIndividual(evt, roisPerWire, rng);
            t.Start();
            wireEntry.BindRawPtr(wireToken, &localWire);
            ROOT::RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }
            t.Stop();
        }
    }
    return t.RealTime();
} 

// Dict-specific hori-spill hit model and token (single class field)
auto CreateDictHoriSpillHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<HitVector>("HitVector");
    ROOT::RFieldToken hitToken = hitModel->GetToken("HitVector");
    return {std::move(hitModel), hitToken};
}

// Dict-specific work function for hori-spill vector
double RunDictHoriSpillWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (totalEntries / nThreads);

    TStopwatch t;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numHoriSpills;
        int spil = idx % numHoriSpills;
        long long uid = static_cast<long long>(evt) * 10000 + spil;
        HitVector localHit = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
        WireVector localWire = generateRandomWireVector(uid, adjustedWiresPerEvent, roisPerWire, rng);
        t.Start();

        // Bind and fill hits
        hitEntry.BindRawPtr(hitToken, &localHit);
        ROOT::RNTupleFillStatus hitStatus;
        hitContext.FillNoFlush(hitEntry, hitStatus);
        if (hitStatus.ShouldFlushCluster()) {
            hitContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
        }

        // Bind and fill wires
        wireEntry.BindRawPtr(wireToken, &localWire);
        ROOT::RNTupleFillStatus wireStatus;
        wireContext.FillNoFlush(wireEntry, wireStatus);
        if (wireStatus.ShouldFlushCluster()) {
            wireContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
        }

        t.Stop();
    }
    return t.RealTime();
} 

// Dict-specific hori-spill individual hit model and token (single class field)
auto CreateDictHoriSpillIndividualHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> {
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<HitIndividual>("HitIndividual");
    ROOT::RFieldToken hitToken = hitModel->GetToken("HitIndividual");
    return {std::move(hitModel), hitToken};
}

// Dict-specific work function for hori-spill individual
double RunDictHoriSpillIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (totalEntries / nThreads);

    TStopwatch t;
    for (int idx = first; idx < last; ++idx) {
        int evt = idx / numHoriSpills;
        int spil = idx % numHoriSpills;
        long long uid = static_cast<long long>(evt) * 10000 + spil;
        // Hits loop
        for (int h = 0; h < adjustedHitsPerEvent; ++h) {
            HitIndividual localHit = generateRandomHitIndividual(uid, rng);
            t.Start();
            hitEntry.BindRawPtr(hitToken, &localHit);
            ROOT::RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }
            t.Stop();
        }
        // Wires loop
        for (int w = 0; w < adjustedWiresPerEvent; ++w) {
            WireIndividual localWire = generateRandomWireIndividual(uid, roisPerWire, rng);
            t.Start();
            wireEntry.BindRawPtr(wireToken, &localWire);
            ROOT::RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }
            t.Stop();
        }
    }
    return t.RealTime();
} 

// Model creation for vector-of-individuals hit
auto CreateVectorOfIndividualsHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> {
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<long long>("EventID");
    ROOT::RFieldToken tEventID = hitModel->GetToken("EventID");
    hitModel->MakeField<std::vector<HitIndividual>>("Hits");
    ROOT::RFieldToken tHits = hitModel->GetToken("Hits");
    std::unordered_map<std::string, ROOT::RFieldToken> tokens = {
        {"EventID", tEventID},
        {"Hits", tHits}
    };
    return {std::move(hitModel), tokens};
}

// Model creation for vector-of-individuals wire
auto CreateVectorOfIndividualsWireModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> {
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    ROOT::RFieldToken tEventID = wireModel->GetToken("EventID");
    wireModel->MakeField<std::vector<WireIndividual>>("Wires");
    ROOT::RFieldToken tWires = wireModel->GetToken("Wires");
    std::unordered_map<std::string, ROOT::RFieldToken> tokens = {
        {"EventID", tEventID},
        {"Wires", tWires}
    };
    return {std::move(wireModel), tokens};
}

// Work function for vector-of-individuals
double RunVectorOfIndividualsWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    const std::unordered_map<std::string, ROOT::RFieldToken>& wireTokens,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
) {
    std::mt19937 rng(seed);
    int th = first / (numEvents / nThreads);

    TStopwatch t;
    for (int evt = first; evt < last; ++evt) {
        std::vector<HitIndividual> localHits;
        localHits.reserve(hitsPerEvent);
        for (int i = 0; i < hitsPerEvent; ++i) localHits.push_back(generateRandomHitIndividual(evt, rng));
        std::vector<WireIndividual> localWires;
        localWires.reserve(wiresPerEvent);
        for (int i = 0; i < wiresPerEvent; ++i) localWires.push_back(generateRandomWireIndividual(evt, roisPerWire, rng));
        long long localEventID = evt;
        t.Start();
        // Bind and fill hits
        hitEntry.BindRawPtr(hitTokens.at("EventID"), &localEventID);
        hitEntry.BindRawPtr(hitTokens.at("Hits"), &localHits);
        ROOT::RNTupleFillStatus hitStatus;
        hitContext.FillNoFlush(hitEntry, hitStatus);
        if (hitStatus.ShouldFlushCluster()) {
            hitContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
        }
        // Bind and fill wires
        wireEntry.BindRawPtr(wireTokens.at("EventID"), &localEventID);
        wireEntry.BindRawPtr(wireTokens.at("Wires"), &localWires);
        ROOT::RNTupleFillStatus wireStatus;
        wireContext.FillNoFlush(wireEntry, wireStatus);
        if (wireStatus.ShouldFlushCluster()) {
            wireContext.FlushColumns();
            { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
        }
        t.Stop();
    }
    return t.RealTime();
} 
