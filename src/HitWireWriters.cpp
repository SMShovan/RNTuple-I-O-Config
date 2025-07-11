#include "HitWireWriters.hpp"
#include "Hit.hpp"
#include "Wire.hpp"
#include "HitWireGenerators.hpp"
#include "Utils.hpp"
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>
#include <ROOT/RNTuple.hxx>
#include <TFile.h>
#include <TStopwatch.h>
#include <filesystem>
#include <thread>
#include <future>
#include <vector>
#include <iostream>
#include <random>
#include <utility>
#include <TObject.h>
#include <mutex>

// NOTE: Adding experimental includes for parallel writing
#include <ROOT/RRawPtrWriteEntry.hxx>
#include <ROOT/RFieldToken.hxx>
#include <ROOT/RNTupleFillContext.hxx>
#include <ROOT/RNTupleFillStatus.hxx>
#include <ROOT/RNTupleParallelWriter.hxx>
#include <ROOT/RNTupleWriteOptions.hxx>

// Import classes from Experimental namespace
using ROOT::Experimental::RNTupleFillContext;
using ROOT::Experimental::RNTupleParallelWriter;
using ROOT::Experimental::Detail::RRawPtrWriteEntry;
using ROOT::RFieldToken;
using ROOT::RNTupleFillStatus;

static int get_nthreads() {
    int n = std::thread::hardware_concurrency();
    return n > 0 ? n : 4;
}

static const std::string kOutputDir = "./output";

// Helper to manage multi-threaded execution
static double executeInParallel(int totalEvents, const std::function<double(int, int, unsigned)>& workFunc) {
    int nThreads = get_nthreads();
    auto seeds = Utils::generateSeeds(nThreads);
    int chunk = totalEvents / nThreads;
    std::vector<std::future<double>> futures;
    for (int th = 0; th < nThreads; ++th) {
        int start = th * chunk;
        int end = (th == nThreads - 1) ? totalEvents : start + chunk;
        futures.push_back(std::async(std::launch::async, workFunc, start, end, seeds[th]));
    }
    double totalTime = 0.0;
    for (auto& f : futures) {
        totalTime += f.get();
    }
    return totalTime;
}

void generateAndWrite_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<HitVector>("HitVector");
    RFieldToken hitToken = hitModel->GetToken("HitVector");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<WireVector>("WireVector");
    RFieldToken wireToken = wireModel->GetToken("WireVector");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch t;
        for (int i = first; i < last; ++i) {
            HitVector localHit = generateRandomHitVector(i, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(i, wiresPerEvent, wiresPerEvent, rng);

            t.Start();

            // Bind and fill hits
            hitEntry.BindRawPtr(hitToken, &localHit);
            RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    hitContext.FlushCluster();
                }
            }

            // Bind and fill wires
            wireEntry.BindRawPtr(wireToken, &localWire);
            RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    wireContext.FlushCluster();
                }
            }

            t.Stop();
        }
        return t.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] Hit/Wire Vector ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_VertiSplit_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and tokens for all fields
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    RFieldToken tEventID = hitModel->GetToken("EventID");
    auto ch = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    RFieldToken tChannel = hitModel->GetToken("Channel");
    auto view = hitModel->MakeField<std::vector<int>>("View");
    RFieldToken tView = hitModel->GetToken("View");
    auto sTick = hitModel->MakeField<std::vector<int>>("StartTick");
    RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto eTick = hitModel->MakeField<std::vector<int>>("EndTick");
    RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto peak = hitModel->MakeField<std::vector<float>>("PeakTime");
    RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto sigmaPeak = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto rms = hitModel->MakeField<std::vector<float>>("RMS");
    RFieldToken tRMS = hitModel->GetToken("RMS");
    auto amp = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto hitADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto integ = hitModel->MakeField<std::vector<float>>("Integral");
    RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto sigmaInt = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto mult = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto locIdx = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto gof = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto ndf = hitModel->MakeField<std::vector<int>>("NDF");
    RFieldToken tNDF = hitModel->GetToken("NDF");
    auto sigType = hitModel->MakeField<std::vector<int>>("SignalType");
    RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto cryo = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto tpc = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto wire = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    RFieldToken tWireVector = wireModel->GetToken("WireVector");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch sw;
        for (int evt = first; evt < last; ++evt) {
            HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, wiresPerEvent, rng);
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
            hitEntry.BindRawPtr(tEventID, &localEventID);
            hitEntry.BindRawPtr(tChannel, &localChannel);
            hitEntry.BindRawPtr(tView, &localView);
            hitEntry.BindRawPtr(tStartTick, &localStartTick);
            hitEntry.BindRawPtr(tEndTick, &localEndTick);
            hitEntry.BindRawPtr(tPeakTime, &localPeakTime);
            hitEntry.BindRawPtr(tSigmaPeakTime, &localSigmaPeakTime);
            hitEntry.BindRawPtr(tRMS, &localRMS);
            hitEntry.BindRawPtr(tPeakAmplitude, &localPeakAmplitude);
            hitEntry.BindRawPtr(tSigmaPeakAmplitude, &localSigmaPeakAmplitude);
            hitEntry.BindRawPtr(tROISummedADC, &localROISummedADC);
            hitEntry.BindRawPtr(tHitSummedADC, &localHitSummedADC);
            hitEntry.BindRawPtr(tIntegral, &localIntegral);
            hitEntry.BindRawPtr(tSigmaIntegral, &localSigmaIntegral);
            hitEntry.BindRawPtr(tMultiplicity, &localMultiplicity);
            hitEntry.BindRawPtr(tLocalIndex, &localLocalIndex);
            hitEntry.BindRawPtr(tGoodnessOfFit, &localGoodnessOfFit);
            hitEntry.BindRawPtr(tNDF, &localNDF);
            hitEntry.BindRawPtr(tSignalType, &localSignalType);
            hitEntry.BindRawPtr(tWireID_Cryostat, &localWireID_Cryostat);
            hitEntry.BindRawPtr(tWireID_TPC, &localWireID_TPC);
            hitEntry.BindRawPtr(tWireID_Plane, &localWireID_Plane);
            hitEntry.BindRawPtr(tWireID_Wire, &localWireID_Wire);
            RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }

            // Bind and fill wires
            wireEntry.BindRawPtr(tWireVector, &localWire);
            RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }

            sw.Stop();
        }
        return sw.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] VertiSplit-Vector ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Vector(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and tokens for all fields
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    RFieldToken tEventID = hitModel->GetToken("EventID");
    auto spilID = hitModel->MakeField<int>("SpilID");
    RFieldToken tSpilID = hitModel->GetToken("SpilID");
    auto ch = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    RFieldToken tChannel = hitModel->GetToken("Channel");
    auto view = hitModel->MakeField<std::vector<int>>("View");
    RFieldToken tView = hitModel->GetToken("View");
    auto sTick = hitModel->MakeField<std::vector<int>>("StartTick");
    RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto eTick = hitModel->MakeField<std::vector<int>>("EndTick");
    RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto peak = hitModel->MakeField<std::vector<float>>("PeakTime");
    RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto sigmaPeak = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto rms = hitModel->MakeField<std::vector<float>>("RMS");
    RFieldToken tRMS = hitModel->GetToken("RMS");
    auto amp = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto hitADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto integ = hitModel->MakeField<std::vector<float>>("Integral");
    RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto sigmaInt = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto mult = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto locIdx = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto gof = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto ndf = hitModel->MakeField<std::vector<int>>("NDF");
    RFieldToken tNDF = hitModel->GetToken("NDF");
    auto sigType = hitModel->MakeField<std::vector<int>>("SignalType");
    RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto cryo = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto tpc = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto wireV = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    RFieldToken tWireVector = wireModel->GetToken("WireVector");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (totalEntries / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch sw;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            HitVector localHit = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(uid, adjustedWiresPerEvent, wiresPerEvent, rng);
            sw.Start();

            // Declare locals for binding
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
            hitEntry.BindRawPtr(tEventID, &localEventID);
            hitEntry.BindRawPtr(tSpilID, &localSpilID);
            hitEntry.BindRawPtr(tChannel, &localChannel);
            hitEntry.BindRawPtr(tView, &localView);
            hitEntry.BindRawPtr(tStartTick, &localStartTick);
            hitEntry.BindRawPtr(tEndTick, &localEndTick);
            hitEntry.BindRawPtr(tPeakTime, &localPeakTime);
            hitEntry.BindRawPtr(tSigmaPeakTime, &localSigmaPeakTime);
            hitEntry.BindRawPtr(tRMS, &localRMS);
            hitEntry.BindRawPtr(tPeakAmplitude, &localPeakAmplitude);
            hitEntry.BindRawPtr(tSigmaPeakAmplitude, &localSigmaPeakAmplitude);
            hitEntry.BindRawPtr(tROISummedADC, &localROISummedADC);
            hitEntry.BindRawPtr(tHitSummedADC, &localHitSummedADC);
            hitEntry.BindRawPtr(tIntegral, &localIntegral);
            hitEntry.BindRawPtr(tSigmaIntegral, &localSigmaIntegral);
            hitEntry.BindRawPtr(tMultiplicity, &localMultiplicity);
            hitEntry.BindRawPtr(tLocalIndex, &localLocalIndex);
            hitEntry.BindRawPtr(tGoodnessOfFit, &localGoodnessOfFit);
            hitEntry.BindRawPtr(tNDF, &localNDF);
            hitEntry.BindRawPtr(tSignalType, &localSignalType);
            hitEntry.BindRawPtr(tWireID_Cryostat, &localWireID_Cryostat);
            hitEntry.BindRawPtr(tWireID_TPC, &localWireID_TPC);
            hitEntry.BindRawPtr(tWireID_Plane, &localWireID_Plane);
            hitEntry.BindRawPtr(tWireID_Wire, &localWireID_Wire);
            RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
            }

            // Bind and fill wires
            wireEntry.BindRawPtr(tWireVector, &localWire);
            RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
            }

            sw.Stop();
        }
        return sw.RealTime();
    };

    double totalTime = executeInParallel(totalEntries, workFunc);
    std::cout << "[Concurrent] HoriSpill-Vector ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and tokens for all fields
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    RFieldToken tEventID = hitModel->GetToken("EventID");
    auto fChannel = hitModel->MakeField<unsigned int>("Channel");
    RFieldToken tChannel = hitModel->GetToken("Channel");
    auto fView = hitModel->MakeField<int>("View");
    RFieldToken tView = hitModel->GetToken("View");
    auto fStartTick = hitModel->MakeField<int>("StartTick");
    RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto fEndTick = hitModel->MakeField<int>("EndTick");
    RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto fPeakTime = hitModel->MakeField<float>("PeakTime");
    RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto fSigmaPeakTime = hitModel->MakeField<float>("SigmaPeakTime");
    RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto fRMS = hitModel->MakeField<float>("RMS");
    RFieldToken tRMS = hitModel->GetToken("RMS");
    auto fPeakAmplitude = hitModel->MakeField<float>("PeakAmplitude");
    RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto fSigmaPeakAmplitude = hitModel->MakeField<float>("SigmaPeakAmplitude");
    RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto fROISummedADC = hitModel->MakeField<float>("ROISummedADC");
    RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto fHitSummedADC = hitModel->MakeField<float>("HitSummedADC");
    RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto fIntegral = hitModel->MakeField<float>("Integral");
    RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto fSigmaIntegral = hitModel->MakeField<float>("SigmaIntegral");
    RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto fMultiplicity = hitModel->MakeField<short int>("Multiplicity");
    RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto fLocalIndex = hitModel->MakeField<short int>("LocalIndex");
    RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto fGoodnessOfFit = hitModel->MakeField<float>("GoodnessOfFit");
    RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto fNDF = hitModel->MakeField<int>("NDF");
    RFieldToken tNDF = hitModel->GetToken("NDF");
    auto fSignalType = hitModel->MakeField<int>("SignalType");
    RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto fWireID_Cryostat = hitModel->MakeField<int>("WireID_Cryostat");
    RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto fWireID_TPC = hitModel->MakeField<int>("WireID_TPC");
    RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto fWireID_Plane = hitModel->MakeField<int>("WireID_Plane");
    RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto fWireID_Wire = hitModel->MakeField<int>("WireID_Wire");
    RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<WireIndividual>("WireIndividual");
    RFieldToken tWireIndividual = wireModel->GetToken("WireIndividual");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch storageTimer;
        for (int eventIndex = first; eventIndex < last; ++eventIndex) {
            for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
                HitIndividual localHit = generateRandomHitIndividual(eventIndex, rng);
                WireIndividual localWire = generateRandomWireIndividual(eventIndex, wiresPerEvent, rng);
                storageTimer.Start();

                // Bind hit fields
                hitEntry.BindRawPtr(tEventID, &localHit.EventID);
                hitEntry.BindRawPtr(tChannel, &localHit.fChannel);
                hitEntry.BindRawPtr(tView, &localHit.fView);
                hitEntry.BindRawPtr(tStartTick, &localHit.fStartTick);
                hitEntry.BindRawPtr(tEndTick, &localHit.fEndTick);
                hitEntry.BindRawPtr(tPeakTime, &localHit.fPeakTime);
                hitEntry.BindRawPtr(tSigmaPeakTime, &localHit.fSigmaPeakTime);
                hitEntry.BindRawPtr(tRMS, &localHit.fRMS);
                hitEntry.BindRawPtr(tPeakAmplitude, &localHit.fPeakAmplitude);
                hitEntry.BindRawPtr(tSigmaPeakAmplitude, &localHit.fSigmaPeakAmplitude);
                hitEntry.BindRawPtr(tROISummedADC, &localHit.fROISummedADC);
                hitEntry.BindRawPtr(tHitSummedADC, &localHit.fHitSummedADC);
                hitEntry.BindRawPtr(tIntegral, &localHit.fIntegral);
                hitEntry.BindRawPtr(tSigmaIntegral, &localHit.fSigmaIntegral);
                hitEntry.BindRawPtr(tMultiplicity, &localHit.fMultiplicity);
                hitEntry.BindRawPtr(tLocalIndex, &localHit.fLocalIndex);
                hitEntry.BindRawPtr(tGoodnessOfFit, &localHit.fGoodnessOfFit);
                hitEntry.BindRawPtr(tNDF, &localHit.fNDF);
                hitEntry.BindRawPtr(tSignalType, &localHit.fSignalType);
                hitEntry.BindRawPtr(tWireID_Cryostat, &localHit.fWireID_Cryostat);
                hitEntry.BindRawPtr(tWireID_TPC, &localHit.fWireID_TPC);
                hitEntry.BindRawPtr(tWireID_Plane, &localHit.fWireID_Plane);
                hitEntry.BindRawPtr(tWireID_Wire, &localHit.fWireID_Wire);
                RNTupleFillStatus hitStatus;
                hitContext.FillNoFlush(hitEntry, hitStatus);
                if (hitStatus.ShouldFlushCluster()) {
                    hitContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
                }

                // Bind wire
                wireEntry.BindRawPtr(tWireIndividual, &localWire);
                RNTupleFillStatus wireStatus;
                wireContext.FillNoFlush(wireEntry, wireStatus);
                if (wireStatus.ShouldFlushCluster()) {
                    wireContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
                }

                storageTimer.Stop();
            }
        }
        return storageTimer.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] Individual ntuples written in " << totalTime * 1000 << " ms" << std::endl;
}

void generateAndWrite_VertiSplit_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    // Hit model with RFieldTokens
    auto hitModel = ROOT::RNTupleModel::Create();
    auto fEventID = hitModel->MakeField<long long>("EventID");
    RFieldToken tEventID = hitModel->GetToken("EventID");
    auto fChannel = hitModel->MakeField<unsigned int>("Channel");
    RFieldToken tChannel = hitModel->GetToken("Channel");
    auto fView = hitModel->MakeField<int>("View");
    RFieldToken tView = hitModel->GetToken("View");
    auto fStartTick = hitModel->MakeField<int>("StartTick");
    RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto fEndTick = hitModel->MakeField<int>("EndTick");
    RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto fPeakTime = hitModel->MakeField<float>("PeakTime");
    RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto fSigmaPeakTime = hitModel->MakeField<float>("SigmaPeakTime");
    RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto fRMS = hitModel->MakeField<float>("RMS");
    RFieldToken tRMS = hitModel->GetToken("RMS");
    auto fPeakAmplitude = hitModel->MakeField<float>("PeakAmplitude");
    RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto fSigmaPeakAmplitude = hitModel->MakeField<float>("SigmaPeakAmplitude");
    RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto fROISummedADC = hitModel->MakeField<float>("ROISummedADC");
    RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto fHitSummedADC = hitModel->MakeField<float>("HitSummedADC");
    RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto fIntegral = hitModel->MakeField<float>("Integral");
    RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto fSigmaIntegral = hitModel->MakeField<float>("SigmaIntegral");
    RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto fMultiplicity = hitModel->MakeField<short int>("Multiplicity");
    RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto fLocalIndex = hitModel->MakeField<short int>("LocalIndex");
    RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto fGoodnessOfFit = hitModel->MakeField<float>("GoodnessOfFit");
    RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto fNDF = hitModel->MakeField<int>("NDF");
    RFieldToken tNDF = hitModel->GetToken("NDF");
    auto fSignalType = hitModel->MakeField<int>("SignalType");
    RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto fWireID_Cryostat = hitModel->MakeField<int>("WireID_Cryostat");
    RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto fWireID_TPC = hitModel->MakeField<int>("WireID_TPC");
    RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto fWireID_Plane = hitModel->MakeField<int>("WireID_Plane");
    RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto fWireID_Wire = hitModel->MakeField<int>("WireID_Wire");
    RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model with RFieldToken
    auto wireModel = ROOT::RNTupleModel::Create();
    auto fWireIndividual = wireModel->MakeField<WireIndividual>("WireIndividual");
    RFieldToken tWireIndividual = wireModel->GetToken("WireIndividual");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    int nThreads = get_nthreads();
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();
        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        int th = first / (numEvents / nThreads);
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch sw;
        for (int evt = first; evt < last; ++evt) {
            for (int i = 0; i < hitsPerEvent; ++i) {
                HitIndividual localHit = generateRandomHitIndividual(evt, rng);
                WireIndividual localWire = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                sw.Start();

                // Bind hit fields using tokens
                hitEntry.BindRawPtr(tEventID, &localHit.EventID);
                hitEntry.BindRawPtr(tChannel, &localHit.fChannel);
                hitEntry.BindRawPtr(tView, &localHit.fView);
                hitEntry.BindRawPtr(tStartTick, &localHit.fStartTick);
                hitEntry.BindRawPtr(tEndTick, &localHit.fEndTick);
                hitEntry.BindRawPtr(tPeakTime, &localHit.fPeakTime);
                hitEntry.BindRawPtr(tSigmaPeakTime, &localHit.fSigmaPeakTime);
                hitEntry.BindRawPtr(tRMS, &localHit.fRMS);
                hitEntry.BindRawPtr(tPeakAmplitude, &localHit.fPeakAmplitude);
                hitEntry.BindRawPtr(tSigmaPeakAmplitude, &localHit.fSigmaPeakAmplitude);
                hitEntry.BindRawPtr(tROISummedADC, &localHit.fROISummedADC);
                hitEntry.BindRawPtr(tHitSummedADC, &localHit.fHitSummedADC);
                hitEntry.BindRawPtr(tIntegral, &localHit.fIntegral);
                hitEntry.BindRawPtr(tSigmaIntegral, &localHit.fSigmaIntegral);
                hitEntry.BindRawPtr(tMultiplicity, &localHit.fMultiplicity);
                hitEntry.BindRawPtr(tLocalIndex, &localHit.fLocalIndex);
                hitEntry.BindRawPtr(tGoodnessOfFit, &localHit.fGoodnessOfFit);
                hitEntry.BindRawPtr(tNDF, &localHit.fNDF);
                hitEntry.BindRawPtr(tSignalType, &localHit.fSignalType);
                hitEntry.BindRawPtr(tWireID_Cryostat, &localHit.fWireID_Cryostat);
                hitEntry.BindRawPtr(tWireID_TPC, &localHit.fWireID_TPC);
                hitEntry.BindRawPtr(tWireID_Plane, &localHit.fWireID_Plane);
                hitEntry.BindRawPtr(tWireID_Wire, &localHit.fWireID_Wire);
                RNTupleFillStatus hitStatus;
                hitContext.FillNoFlush(hitEntry, hitStatus);
                if (hitStatus.ShouldFlushCluster()) {
                    hitContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
                }

                // Bind wire
                wireEntry.BindRawPtr(tWireIndividual, &localWire);
                RNTupleFillStatus wireStatus;
                wireContext.FillNoFlush(wireEntry, wireStatus);
                if (wireStatus.ShouldFlushCluster()) {
                    wireContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
                }

                sw.Stop();
            }
        }
        return sw.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] VertiSplit-Individual ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Individual(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and tokens for all fields
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    RFieldToken tEventID = hitModel->GetToken("EventID");
    auto spilID = hitModel->MakeField<int>("SpilID");
    RFieldToken tSpilID = hitModel->GetToken("SpilID");
    auto ch = hitModel->MakeField<unsigned int>("Channel");
    RFieldToken tChannel = hitModel->GetToken("Channel");
    auto view = hitModel->MakeField<int>("View");
    RFieldToken tView = hitModel->GetToken("View");
    auto sTick = hitModel->MakeField<int>("StartTick");
    RFieldToken tStartTick = hitModel->GetToken("StartTick");
    auto eTick = hitModel->MakeField<int>("EndTick");
    RFieldToken tEndTick = hitModel->GetToken("EndTick");
    auto peak = hitModel->MakeField<float>("PeakTime");
    RFieldToken tPeakTime = hitModel->GetToken("PeakTime");
    auto sigmaPeak = hitModel->MakeField<float>("SigmaPeakTime");
    RFieldToken tSigmaPeakTime = hitModel->GetToken("SigmaPeakTime");
    auto rms = hitModel->MakeField<float>("RMS");
    RFieldToken tRMS = hitModel->GetToken("RMS");
    auto amp = hitModel->MakeField<float>("PeakAmplitude");
    RFieldToken tPeakAmplitude = hitModel->GetToken("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<float>("SigmaPeakAmplitude");
    RFieldToken tSigmaPeakAmplitude = hitModel->GetToken("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<float>("ROISummedADC");
    RFieldToken tROISummedADC = hitModel->GetToken("ROISummedADC");
    auto hitADC = hitModel->MakeField<float>("HitSummedADC");
    RFieldToken tHitSummedADC = hitModel->GetToken("HitSummedADC");
    auto integ = hitModel->MakeField<float>("Integral");
    RFieldToken tIntegral = hitModel->GetToken("Integral");
    auto sigmaInt = hitModel->MakeField<float>("SigmaIntegral");
    RFieldToken tSigmaIntegral = hitModel->GetToken("SigmaIntegral");
    auto mult = hitModel->MakeField<short int>("Multiplicity");
    RFieldToken tMultiplicity = hitModel->GetToken("Multiplicity");
    auto locIdx = hitModel->MakeField<short int>("LocalIndex");
    RFieldToken tLocalIndex = hitModel->GetToken("LocalIndex");
    auto gof = hitModel->MakeField<float>("GoodnessOfFit");
    RFieldToken tGoodnessOfFit = hitModel->GetToken("GoodnessOfFit");
    auto ndf = hitModel->MakeField<int>("NDF");
    RFieldToken tNDF = hitModel->GetToken("NDF");
    auto sigType = hitModel->MakeField<int>("SignalType");
    RFieldToken tSignalType = hitModel->GetToken("SignalType");
    auto cryo = hitModel->MakeField<int>("WireID_Cryostat");
    RFieldToken tWireID_Cryostat = hitModel->GetToken("WireID_Cryostat");
    auto tpc = hitModel->MakeField<int>("WireID_TPC");
    RFieldToken tWireID_TPC = hitModel->GetToken("WireID_TPC");
    auto plane = hitModel->MakeField<int>("WireID_Plane");
    RFieldToken tWireID_Plane = hitModel->GetToken("WireID_Plane");
    auto wire = hitModel->MakeField<int>("WireID_Wire");
    RFieldToken tWireID_Wire = hitModel->GetToken("WireID_Wire");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    RFieldToken tWireIndividual = wireModel->GetToken("WireIndividual");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (totalEntries / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch sw;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            for (int h = 0; h < adjustedHitsPerEvent; ++h) {
                HitIndividual localHit = generateRandomHitIndividual(uid, rng);
                WireIndividual localWire = generateRandomWireIndividual(uid, wiresPerEvent, rng);
                sw.Start();

                int localSpilID = spil;

                // Bind hit fields
                hitEntry.BindRawPtr(tEventID, &localHit.EventID);
                hitEntry.BindRawPtr(tSpilID, &localSpilID);
                hitEntry.BindRawPtr(tChannel, &localHit.fChannel);
                hitEntry.BindRawPtr(tView, &localHit.fView);
                hitEntry.BindRawPtr(tStartTick, &localHit.fStartTick);
                hitEntry.BindRawPtr(tEndTick, &localHit.fEndTick);
                hitEntry.BindRawPtr(tPeakTime, &localHit.fPeakTime);
                hitEntry.BindRawPtr(tSigmaPeakTime, &localHit.fSigmaPeakTime);
                hitEntry.BindRawPtr(tRMS, &localHit.fRMS);
                hitEntry.BindRawPtr(tPeakAmplitude, &localHit.fPeakAmplitude);
                hitEntry.BindRawPtr(tSigmaPeakAmplitude, &localHit.fSigmaPeakAmplitude);
                hitEntry.BindRawPtr(tROISummedADC, &localHit.fROISummedADC);
                hitEntry.BindRawPtr(tHitSummedADC, &localHit.fHitSummedADC);
                hitEntry.BindRawPtr(tIntegral, &localHit.fIntegral);
                hitEntry.BindRawPtr(tSigmaIntegral, &localHit.fSigmaIntegral);
                hitEntry.BindRawPtr(tMultiplicity, &localHit.fMultiplicity);
                hitEntry.BindRawPtr(tLocalIndex, &localHit.fLocalIndex);
                hitEntry.BindRawPtr(tGoodnessOfFit, &localHit.fGoodnessOfFit);
                hitEntry.BindRawPtr(tNDF, &localHit.fNDF);
                hitEntry.BindRawPtr(tSignalType, &localHit.fSignalType);
                hitEntry.BindRawPtr(tWireID_Cryostat, &localHit.fWireID_Cryostat);
                hitEntry.BindRawPtr(tWireID_TPC, &localHit.fWireID_TPC);
                hitEntry.BindRawPtr(tWireID_Plane, &localHit.fWireID_Plane);
                hitEntry.BindRawPtr(tWireID_Wire, &localHit.fWireID_Wire);
                RNTupleFillStatus hitStatus;
                hitContext.FillNoFlush(hitEntry, hitStatus);
                if (hitStatus.ShouldFlushCluster()) {
                    hitContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
                }

                // Bind wire
                wireEntry.BindRawPtr(tWireIndividual, &localWire);
                RNTupleFillStatus wireStatus;
                wireContext.FillNoFlush(wireEntry, wireStatus);
                if (wireStatus.ShouldFlushCluster()) {
                    wireContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
                }

                sw.Stop();
            }
        }
        return sw.RealTime();
    };

    double totalTime = executeInParallel(totalEntries, workFunc);
    std::cout << "[Concurrent] HoriSpill-Individual ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitVector>("HitVector");
    RFieldToken hitToken = hitModel->GetToken("HitVector");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    RFieldToken wireToken = wireModel->GetToken("WireVector");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, rng);
            t.Start();

            // Bind and fill hits
            hitEntry.BindRawPtr(hitToken, &localHit);
            RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    hitContext.FlushCluster();
                }
            }

            // Bind and fill wires
            wireEntry.BindRawPtr(wireToken, &localWire);
            RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    wireContext.FlushCluster();
                }
            }

            t.Stop();
        }
        return t.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] Vector-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitIndividual>("HitIndividual");
    RFieldToken tHitIndividual = hitModel->GetToken("HitIndividual");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    RFieldToken tWireIndividual = wireModel->GetToken("WireIndividual");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            for (int i = 0; i < hitsPerEvent; ++i) {
                HitIndividual localHit = generateRandomHitIndividual(evt, rng);
                WireIndividual localWire = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                t.Start();

                // Bind and fill hit
                hitEntry.BindRawPtr(tHitIndividual, &localHit);
                RNTupleFillStatus hitStatus;
                hitContext.FillNoFlush(hitEntry, hitStatus);
                if (hitStatus.ShouldFlushCluster()) {
                    hitContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
                }

                // Bind and fill wire
                wireEntry.BindRawPtr(tWireIndividual, &localWire);
                RNTupleFillStatus wireStatus;
                wireContext.FillNoFlush(wireEntry, wireStatus);
                if (wireStatus.ShouldFlushCluster()) {
                    wireContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
                }

                t.Stop();
            }
        }
        return t.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] Individual-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitVector>("HitVector");
    RFieldToken hitToken = hitModel->GetToken("HitVector");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    RFieldToken wireToken = wireModel->GetToken("WireVector");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, wiresPerEvent, rng);
            t.Start();

            // Bind and fill hits
            hitEntry.BindRawPtr(hitToken, &localHit);
            RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    hitContext.FlushCluster();
                }
            }

            // Bind and fill wires
            wireEntry.BindRawPtr(wireToken, &localWire);
            RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    wireContext.FlushCluster();
                }
            }

            t.Stop();
        }
        return t.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] VertiSplit-Vector-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitIndividual>("HitIndividual");
    RFieldToken tHitIndividual = hitModel->GetToken("HitIndividual");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    RFieldToken tWireIndividual = wireModel->GetToken("WireIndividual");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch sw;
        for (int evt = first; evt < last; ++evt) {
            for (int h = 0; h < hitsPerEvent; ++h) {
                HitIndividual localHit = generateRandomHitIndividual(evt, rng);
                WireIndividual localWire = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                sw.Start();

                // Bind and fill hit
                hitEntry.BindRawPtr(tHitIndividual, &localHit);
                RNTupleFillStatus hitStatus;
                hitContext.FillNoFlush(hitEntry, hitStatus);
                if (hitStatus.ShouldFlushCluster()) {
                    hitContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
                }

                // Bind and fill wire
                wireEntry.BindRawPtr(tWireIndividual, &localWire);
                RNTupleFillStatus wireStatus;
                wireContext.FillNoFlush(wireEntry, wireStatus);
                if (wireStatus.ShouldFlushCluster()) {
                    wireContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
                }

                sw.Stop();
            }
        }
        return sw.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] VertiSplit-Individual-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitVector>("HitVector");
    RFieldToken hitToken = hitModel->GetToken("HitVector");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    RFieldToken wireToken = wireModel->GetToken("WireVector");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (totalEntries / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch t;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            HitVector localHit = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(uid, adjustedWiresPerEvent, wiresPerEvent, rng);
            t.Start();

            // Bind and fill hits
            hitEntry.BindRawPtr(hitToken, &localHit);
            RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    hitContext.FlushCluster();
                }
            }

            // Bind and fill wires
            wireEntry.BindRawPtr(wireToken, &localWire);
            RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    wireContext.FlushCluster();
                }
            }

            t.Stop();
        }
        return t.RealTime();
    };

    double totalTime = executeInParallel(totalEntries, workFunc);
    std::cout << "[Concurrent] HoriSpill-Vector-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitIndividual>("HitIndividual");
    RFieldToken tHitIndividual = hitModel->GetToken("HitIndividual");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and token
    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    RFieldToken tWireIndividual = wireModel->GetToken("WireIndividual");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (totalEntries / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];

        TStopwatch sw;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            for (int h = 0; h < adjustedHitsPerEvent; ++h) {
                HitIndividual localHit = generateRandomHitIndividual(uid, rng);
                WireIndividual localWire = generateRandomWireIndividual(uid, wiresPerEvent, rng);
                sw.Start();

                // Bind and fill hit
                hitEntry.BindRawPtr(tHitIndividual, &localHit);
                RNTupleFillStatus hitStatus;
                hitContext.FillNoFlush(hitEntry, hitStatus);
                if (hitStatus.ShouldFlushCluster()) {
                    hitContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); hitContext.FlushCluster(); }
                }

                // Bind and fill wire
                wireEntry.BindRawPtr(tWireIndividual, &localWire);
                RNTupleFillStatus wireStatus;
                wireContext.FillNoFlush(wireEntry, wireStatus);
                if (wireStatus.ShouldFlushCluster()) {
                    wireContext.FlushColumns();
                    { std::lock_guard<std::mutex> lock(mutex); wireContext.FlushCluster(); }
                }

                sw.Stop();
            }
        }
        return sw.RealTime();
    };

    double totalTime = executeInParallel(totalEntries, workFunc);
    std::cout << "[Concurrent] HoriSpill-Individual-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Vector_Of_Individuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and tokens
    auto hitModel = ROOT::RNTupleModel::Create();
    hitModel->MakeField<long long>("EventID");
    RFieldToken tEventID = hitModel->GetToken("EventID");
    hitModel->MakeField<std::vector<HitIndividual>>("Hits");
    RFieldToken tHits = hitModel->GetToken("Hits");
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);

    // Wire model and tokens
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    RFieldToken tWireEventID = wireModel->GetToken("EventID");
    wireModel->MakeField<std::vector<WireIndividual>>("Wires");
    RFieldToken tWires = wireModel->GetToken("Wires");
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Determine number of threads
    int nThreads = get_nthreads();

    // Create fill contexts and entries per thread for hits and wires
    std::vector<std::shared_ptr<RNTupleFillContext>> hitContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> hitEntries(nThreads);
    std::vector<std::shared_ptr<RNTupleFillContext>> wireContexts(nThreads);
    std::vector<std::unique_ptr<RRawPtrWriteEntry>> wireEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitContexts[th] = hitWriter->CreateFillContext();
        hitEntries[th] = hitContexts[th]->GetModel().CreateRawPtrWriteEntry();

        wireContexts[th] = wireWriter->CreateFillContext();
        wireEntries[th] = wireContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        // Compute thread index (assuming even distribution)
        int th = first / (numEvents / nThreads); // Approximate thread index
        auto& hitContext = *hitContexts[th];
        auto& hitEntry = *hitEntries[th];
        auto& wireContext = *wireContexts[th];
        auto& wireEntry = *wireEntries[th];
        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            std::vector<HitIndividual> localHits;
            localHits.reserve(hitsPerEvent);
            for (int i = 0; i < hitsPerEvent; ++i) localHits.push_back(generateRandomHitIndividual(evt, rng));
            std::vector<WireIndividual> localWires;
            localWires.reserve(wiresPerEvent);
            for (int i = 0; i < wiresPerEvent; ++i) localWires.push_back(generateRandomWireIndividual(evt, wiresPerEvent, rng));
            long long localEventID = evt;
            t.Start();
            // Bind and fill hits
            hitEntry.BindRawPtr(tEventID, &localEventID);
            hitEntry.BindRawPtr(tHits, &localHits);
            RNTupleFillStatus hitStatus;
            hitContext.FillNoFlush(hitEntry, hitStatus);
            if (hitStatus.ShouldFlushCluster()) {
                hitContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    hitContext.FlushCluster();
                }
            }
            // Bind and fill wires
            wireEntry.BindRawPtr(tWireEventID, &localEventID);
            wireEntry.BindRawPtr(tWires, &localWires);
            RNTupleFillStatus wireStatus;
            wireContext.FillNoFlush(wireEntry, wireStatus);
            if (wireStatus.ShouldFlushCluster()) {
                wireContext.FlushColumns();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    wireContext.FlushCluster();
                }
            }
            t.Stop();
        }
        return t.RealTime();
    };

    double totalTime = executeInParallel(numEvents, workFunc);
    std::cout << "[Concurrent] Vector-of-Individuals ntuples written in " << totalTime * 1000 << " ms\n";
}

void out() {
    int numEvents = 1000;
    int hitsPerEvent = 100;
    int wiresPerEvent = 100;
    int numHoriSpills = 10;

    std::cout << "Generating HitWire data with Vector format..." << std::endl;
    generateAndWrite_Hit_Wire_Vector(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector.root");
    std::cout << "Generating HitWire data with Individual format..." << std::endl;
    generateAndWrite_Hit_Wire_Individual(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual.root");
    std::cout << "Generating VertiSplit HitWire data with Vector format..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Vector(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_vector.root");
    std::cout << "Generating VertiSplit HitWire data with Individual format..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Individual(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_individual.root");
    std::cout << "Generating HoriSpill HitWire data with Vector format..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Vector(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_vector.root");
    std::cout << "Generating HoriSpill HitWire data with Individual format..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Individual(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_individual.root");
    //--- DICTIONARY-BASED EXPERIMENTS ---
    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Generating HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWrite_Hit_Wire_Vector_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_dict.root");
    std::cout << "Generating HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWrite_Hit_Wire_Individual_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual_dict.root");
    std::cout << "Generating VertiSplit HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_vector_dict.root");
    std::cout << "Generating VertiSplit HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_individual_dict.root");
    std::cout << "Generating HoriSpill HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_vector_dict.root");
    std::cout << "Generating HoriSpill HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_individual_dict.root");
    std::cout << "Generating HitWire data with Vector of Individuals format..." << std::endl;
    generateAndWrite_Hit_Wire_Vector_Of_Individuals(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_of_individuals.root");
}
