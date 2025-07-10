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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitVector>("HitVector");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch t;
        for (int i = first; i < last; ++i) {
            HitVector localHit = generateRandomHitVector(i, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(i, wiresPerEvent, wiresPerEvent, rng);
            t.Start();
            {
                std::lock_guard<std::mutex> lock(mutex);
                *hitDataPtr = std::move(localHit);
                *wireDataPtr = std::move(localWire);
                hitWriter->Fill();
                wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    auto ch = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    auto view = hitModel->MakeField<std::vector<int>>("View");
    auto sTick = hitModel->MakeField<std::vector<int>>("StartTick");
    auto eTick = hitModel->MakeField<std::vector<int>>("EndTick");
    auto peak = hitModel->MakeField<std::vector<float>>("PeakTime");
    auto sigmaPeak = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    auto rms = hitModel->MakeField<std::vector<float>>("RMS");
    auto amp = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    auto hitADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    auto integ = hitModel->MakeField<std::vector<float>>("Integral");
    auto sigmaInt = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    auto mult = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    auto locIdx = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    auto gof = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    auto ndf = hitModel->MakeField<std::vector<int>>("NDF");
    auto sigType = hitModel->MakeField<std::vector<int>>("SignalType");
    auto cryo = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    auto tpc = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    auto plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    auto wire = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch sw;
        for (int evt = first; evt < last; ++evt) {
            HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, wiresPerEvent, rng);
            sw.Start();
            {
                std::lock_guard<std::mutex> lock(mutex);
                *eventID = localHit.EventID;
                *ch = std::move(localHit.getChannel());
                *view = std::move(localHit.getView());
                *sTick = std::move(localHit.getStartTick());
                *eTick = std::move(localHit.getEndTick());
                *peak = std::move(localHit.getPeakTime());
                *sigmaPeak = std::move(localHit.getSigmaPeakTime());
                *rms = std::move(localHit.getRMS());
                *amp = std::move(localHit.getPeakAmplitude());
                *sigmaAmp = std::move(localHit.getSigmaPeakAmplitude());
                *roiADC = std::move(localHit.getROISummedADC());
                *hitADC = std::move(localHit.getHitSummedADC());
                *integ = std::move(localHit.getIntegral());
                *sigmaInt = std::move(localHit.getSigmaIntegral());
                *mult = std::move(localHit.getMultiplicity());
                *locIdx = std::move(localHit.getLocalIndex());
                *gof = std::move(localHit.getGoodnessOfFit());
                *ndf = std::move(localHit.getNDF());
                *sigType = std::move(localHit.getSignalType());
                *cryo = std::move(localHit.getWireID_Cryostat());
                *tpc = std::move(localHit.getWireID_TPC());
                *plane = std::move(localHit.getWireID_Plane());
                *wire = std::move(localHit.getWireID_Wire());
                *wireDataPtr = std::move(localWire);
                hitWriter->Fill();
                wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    auto spilID = hitModel->MakeField<int>("SpilID");
    auto ch = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    auto view = hitModel->MakeField<std::vector<int>>("View");
    auto sTick = hitModel->MakeField<std::vector<int>>("StartTick");
    auto eTick = hitModel->MakeField<std::vector<int>>("EndTick");
    auto peak = hitModel->MakeField<std::vector<float>>("PeakTime");
    auto sigmaPeak = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    auto rms = hitModel->MakeField<std::vector<float>>("RMS");
    auto amp = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    auto hitADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    auto integ = hitModel->MakeField<std::vector<float>>("Integral");
    auto sigmaInt = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    auto mult = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    auto locIdx = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    auto gof = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    auto ndf = hitModel->MakeField<std::vector<int>>("NDF");
    auto sigType = hitModel->MakeField<std::vector<int>>("SignalType");
    auto cryo = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    auto tpc = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    auto plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    auto wireV = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch sw;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            HitVector localHit = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(uid, adjustedWiresPerEvent, wiresPerEvent, rng);
            sw.Start();
            {
                std::lock_guard<std::mutex> lock(mutex);
                *eventID = localHit.EventID;
                *spilID = spil;
                *ch = std::move(localHit.getChannel());
                *view = std::move(localHit.getView());
                *sTick = std::move(localHit.getStartTick());
                *eTick = std::move(localHit.getEndTick());
                *peak = std::move(localHit.getPeakTime());
                *sigmaPeak = std::move(localHit.getSigmaPeakTime());
                *rms = std::move(localHit.getRMS());
                *amp = std::move(localHit.getPeakAmplitude());
                *sigmaAmp = std::move(localHit.getSigmaPeakAmplitude());
                *roiADC = std::move(localHit.getROISummedADC());
                *hitADC = std::move(localHit.getHitSummedADC());
                *integ = std::move(localHit.getIntegral());
                *sigmaInt = std::move(localHit.getSigmaIntegral());
                *mult = std::move(localHit.getMultiplicity());
                *locIdx = std::move(localHit.getLocalIndex());
                *gof = std::move(localHit.getGoodnessOfFit());
                *ndf = std::move(localHit.getNDF());
                *sigType = std::move(localHit.getSignalType());
                *cryo = std::move(localHit.getWireID_Cryostat());
                *tpc = std::move(localHit.getWireID_TPC());
                *plane = std::move(localHit.getWireID_Plane());
                *wireV = std::move(localHit.getWireID_Wire());
                *wireDataPtr = std::move(localWire);
                hitWriter->Fill();
                wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    auto fChannel = hitModel->MakeField<unsigned int>("Channel");
    auto fView = hitModel->MakeField<int>("View");
    auto fStartTick = hitModel->MakeField<int>("StartTick");
    auto fEndTick = hitModel->MakeField<int>("EndTick");
    auto fPeakTime = hitModel->MakeField<float>("PeakTime");
    auto fSigmaPeakTime = hitModel->MakeField<float>("SigmaPeakTime");
    auto fRMS = hitModel->MakeField<float>("RMS");
    auto fPeakAmplitude = hitModel->MakeField<float>("PeakAmplitude");
    auto fSigmaPeakAmplitude = hitModel->MakeField<float>("SigmaPeakAmplitude");
    auto fROISummedADC = hitModel->MakeField<float>("ROISummedADC");
    auto fHitSummedADC = hitModel->MakeField<float>("HitSummedADC");
    auto fIntegral = hitModel->MakeField<float>("Integral");
    auto fSigmaIntegral = hitModel->MakeField<float>("SigmaIntegral");
    auto fMultiplicity = hitModel->MakeField<short int>("Multiplicity");
    auto fLocalIndex = hitModel->MakeField<short int>("LocalIndex");
    auto fGoodnessOfFit = hitModel->MakeField<float>("GoodnessOfFit");
    auto fNDF = hitModel->MakeField<int>("NDF");
    auto fSignalType = hitModel->MakeField<int>("SignalType");
    auto fWireID_Cryostat = hitModel->MakeField<int>("WireID_Cryostat");
    auto fWireID_TPC = hitModel->MakeField<int>("WireID_TPC");
    auto fWireID_Plane = hitModel->MakeField<int>("WireID_Plane");
    auto fWireID_Wire = hitModel->MakeField<int>("WireID_Wire");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch storageTimer;
        for (int eventIndex = first; eventIndex < last; ++eventIndex) {
            for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
                HitIndividual localHit = generateRandomHitIndividual(eventIndex, rng);
                WireIndividual localWire = generateRandomWireIndividual(eventIndex, wiresPerEvent, rng);
                storageTimer.Start();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    *eventID = localHit.EventID;
                    *fChannel = localHit.fChannel;
                    *fView = localHit.fView;
                    *fStartTick = localHit.fStartTick;
                    *fEndTick = localHit.fEndTick;
                    *fPeakTime = localHit.fPeakTime;
                    *fSigmaPeakTime = localHit.fSigmaPeakTime;
                    *fRMS = localHit.fRMS;
                    *fPeakAmplitude = localHit.fPeakAmplitude;
                    *fSigmaPeakAmplitude = localHit.fSigmaPeakAmplitude;
                    *fROISummedADC = localHit.fROISummedADC;
                    *fHitSummedADC = localHit.fHitSummedADC;
                    *fIntegral = localHit.fIntegral;
                    *fSigmaIntegral = localHit.fSigmaIntegral;
                    *fMultiplicity = localHit.fMultiplicity;
                    *fLocalIndex = localHit.fLocalIndex;
                    *fGoodnessOfFit = localHit.fGoodnessOfFit;
                    *fNDF = localHit.fNDF;
                    *fSignalType = localHit.fSignalType;
                    *fWireID_Cryostat = localHit.fWireID_Cryostat;
                    *fWireID_TPC = localHit.fWireID_TPC;
                    *fWireID_Plane = localHit.fWireID_Plane;
                    *fWireID_Wire = localHit.fWireID_Wire;
                    *wireDataPtr = std::move(localWire);
                    hitWriter->Fill();
                    wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    auto ch = hitModel->MakeField<unsigned int>("Channel");
    auto view = hitModel->MakeField<int>("View");
    auto sTick = hitModel->MakeField<int>("StartTick");
    auto eTick = hitModel->MakeField<int>("EndTick");
    auto peak = hitModel->MakeField<float>("PeakTime");
    auto sigmaPeak = hitModel->MakeField<float>("SigmaPeakTime");
    auto rms = hitModel->MakeField<float>("RMS");
    auto amp = hitModel->MakeField<float>("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<float>("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<float>("ROISummedADC");
    auto hitADC = hitModel->MakeField<float>("HitSummedADC");
    auto integ = hitModel->MakeField<float>("Integral");
    auto sigmaInt = hitModel->MakeField<float>("SigmaIntegral");
    auto mult = hitModel->MakeField<short int>("Multiplicity");
    auto locIdx = hitModel->MakeField<short int>("LocalIndex");
    auto gof = hitModel->MakeField<float>("GoodnessOfFit");
    auto ndf = hitModel->MakeField<int>("NDF");
    auto sigType = hitModel->MakeField<int>("SignalType");
    auto cryo = hitModel->MakeField<int>("WireID_Cryostat");
    auto tpc = hitModel->MakeField<int>("WireID_TPC");
    auto plane = hitModel->MakeField<int>("WireID_Plane");
    auto wire = hitModel->MakeField<int>("WireID_Wire");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch sw;
        for (int evt = first; evt < last; ++evt) {
            for (int i = 0; i < hitsPerEvent; ++i) {
                HitIndividual localHit = generateRandomHitIndividual(evt, rng);
                WireIndividual localWire = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                sw.Start();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    *eventID = localHit.EventID;
                    *ch = localHit.fChannel;
                    *view = localHit.fView;
                    *sTick = localHit.fStartTick;
                    *eTick = localHit.fEndTick;
                    *peak = localHit.fPeakTime;
                    *sigmaPeak = localHit.fSigmaPeakTime;
                    *rms = localHit.fRMS;
                    *amp = localHit.fPeakAmplitude;
                    *sigmaAmp = localHit.fSigmaPeakAmplitude;
                    *roiADC = localHit.fROISummedADC;
                    *hitADC = localHit.fHitSummedADC;
                    *integ = localHit.fIntegral;
                    *sigmaInt = localHit.fSigmaIntegral;
                    *mult = localHit.fMultiplicity;
                    *locIdx = localHit.fLocalIndex;
                    *gof = localHit.fGoodnessOfFit;
                    *ndf = localHit.fNDF;
                    *sigType = localHit.fSignalType;
                    *cryo = localHit.fWireID_Cryostat;
                    *tpc = localHit.fWireID_TPC;
                    *plane = localHit.fWireID_Plane;
                    *wire = localHit.fWireID_Wire;
                    *wireDataPtr = std::move(localWire);
                    hitWriter->Fill();
                    wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    auto spilID = hitModel->MakeField<int>("SpilID");
    auto ch = hitModel->MakeField<unsigned int>("Channel");
    auto view = hitModel->MakeField<int>("View");
    auto sTick = hitModel->MakeField<int>("StartTick");
    auto eTick = hitModel->MakeField<int>("EndTick");
    auto peak = hitModel->MakeField<float>("PeakTime");
    auto sigmaPk = hitModel->MakeField<float>("SigmaPeakTime");
    auto rms = hitModel->MakeField<float>("RMS");
    auto amp = hitModel->MakeField<float>("PeakAmplitude");
    auto sigmaAmp = hitModel->MakeField<float>("SigmaPeakAmplitude");
    auto roiADC = hitModel->MakeField<float>("ROISummedADC");
    auto hitADC = hitModel->MakeField<float>("HitSummedADC");
    auto integ = hitModel->MakeField<float>("Integral");
    auto sigmaInt = hitModel->MakeField<float>("SigmaIntegral");
    auto mult = hitModel->MakeField<short int>("Multiplicity");
    auto locIdx = hitModel->MakeField<short int>("LocalIndex");
    auto gof = hitModel->MakeField<float>("GoodnessOfFit");
    auto ndf = hitModel->MakeField<int>("NDF");
    auto sigType = hitModel->MakeField<int>("SignalType");
    auto cryo = hitModel->MakeField<int>("WireID_Cryostat");
    auto tpc = hitModel->MakeField<int>("WireID_TPC");
    auto plane = hitModel->MakeField<int>("WireID_Plane");
    auto wire = hitModel->MakeField<int>("WireID_Wire");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch sw;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            for (int h = 0; h < adjustedHitsPerEvent; ++h) {
                HitIndividual localHit = generateRandomHitIndividual(uid, rng);
                WireIndividual localWire = generateRandomWireIndividual(uid, wiresPerEvent, rng);
                sw.Start();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    *eventID = localHit.EventID;
                    *spilID = spil;
                    *ch = localHit.fChannel;
                    *view = localHit.fView;
                    *sTick = localHit.fStartTick;
                    *eTick = localHit.fEndTick;
                    *peak = localHit.fPeakTime;
                    *sigmaPk = localHit.fSigmaPeakTime;
                    *rms = localHit.fRMS;
                    *amp = localHit.fPeakAmplitude;
                    *sigmaAmp = localHit.fSigmaPeakAmplitude;
                    *roiADC = localHit.fROISummedADC;
                    *hitADC = localHit.fHitSummedADC;
                    *integ = localHit.fIntegral;
                    *sigmaInt = localHit.fSigmaIntegral;
                    *mult = localHit.fMultiplicity;
                    *locIdx = localHit.fLocalIndex;
                    *gof = localHit.fGoodnessOfFit;
                    *ndf = localHit.fNDF;
                    *sigType = localHit.fSignalType;
                    *cryo = localHit.fWireID_Cryostat;
                    *tpc = localHit.fWireID_TPC;
                    *plane = localHit.fWireID_Plane;
                    *wire = localHit.fWireID_Wire;
                    *wireDataPtr = std::move(localWire);
                    hitWriter->Fill();
                    wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitVector>("HitVector");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, rng);
            t.Start();
            {
                std::lock_guard<std::mutex> lock(mutex);
                *hitDataPtr = std::move(localHit);
                *wireDataPtr = std::move(localWire);
                hitWriter->Fill();
                wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitIndividual>("HitIndividual");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            for (int i = 0; i < hitsPerEvent; ++i) {
                HitIndividual localHit = generateRandomHitIndividual(evt, rng);
                WireIndividual localWire = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                t.Start();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    *hitDataPtr = std::move(localHit);
                    *wireDataPtr = std::move(localWire);
                    hitWriter->Fill();
                    wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitVector>("HitVector");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            HitVector localHit = generateRandomHitVector(evt, hitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(evt, wiresPerEvent, wiresPerEvent, rng);
            t.Start();
            {
                std::lock_guard<std::mutex> lock(mutex);
                *hitDataPtr = std::move(localHit);
                *wireDataPtr = std::move(localWire);
                hitWriter->Fill();
                wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitIndividual>("HitIndividual");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch sw;
        for (int evt = first; evt < last; ++evt) {
            for (int h = 0; h < hitsPerEvent; ++h) {
                HitIndividual localHit = generateRandomHitIndividual(evt, rng);
                WireIndividual localWire = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                sw.Start();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    *hitDataPtr = std::move(localHit);
                    *wireDataPtr = std::move(localWire);
                    hitWriter->Fill();
                    wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitVector>("HitVector");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireVector>("WireVector");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch t;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            HitVector localHit = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
            WireVector localWire = generateRandomWireVector(uid, adjustedWiresPerEvent, wiresPerEvent, rng);
            t.Start();
            {
                std::lock_guard<std::mutex> lock(mutex);
                *hitDataPtr = std::move(localHit);
                *wireDataPtr = std::move(localWire);
                hitWriter->Fill();
                wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitDataPtr = hitModel->MakeField<HitIndividual>("HitIndividual");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireDataPtr = wireModel->MakeField<WireIndividual>("WireIndividual");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    int totalEntries = numEvents * numHoriSpills;
    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch sw;
        for (int idx = first; idx < last; ++idx) {
            int evt = idx / numHoriSpills;
            int spil = idx % numHoriSpills;
            long long uid = static_cast<long long>(evt) * 10000 + spil;
            for (int h = 0; h < adjustedHitsPerEvent; ++h) {
                HitIndividual localHit = generateRandomHitIndividual(uid, rng);
                WireIndividual localWire = generateRandomWireIndividual(uid, wiresPerEvent, rng);
                sw.Start();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    *hitDataPtr = std::move(localHit);
                    *wireDataPtr = std::move(localWire);
                    hitWriter->Fill();
                    wireWriter->Fill();
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

    auto hitModel = ROOT::RNTupleModel::Create();
    auto hitEventID = hitModel->MakeField<long long>("EventID");
    auto hitsVec = hitModel->MakeField<std::vector<HitIndividual>>("Hits");
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "hits", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto wireEventID = wireModel->MakeField<long long>("EventID");
    auto wiresVec = wireModel->MakeField<std::vector<WireIndividual>>("Wires");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "wires", *file);

    auto workFunc = [&](int first, int last, unsigned seed) {
        std::mt19937 rng(seed);
        TStopwatch t;
        for (int evt = first; evt < last; ++evt) {
            std::vector<HitIndividual> localHits;
            localHits.reserve(hitsPerEvent);
            for (int i = 0; i < hitsPerEvent; ++i) localHits.push_back(generateRandomHitIndividual(evt, rng));
            std::vector<WireIndividual> localWires;
            localWires.reserve(wiresPerEvent);
            for (int i = 0; i < wiresPerEvent; ++i) localWires.push_back(generateRandomWireIndividual(evt, wiresPerEvent, rng));
            t.Start();
            {
                std::lock_guard<std::mutex> lock(mutex);
                *hitEventID = evt;
                *hitsVec = std::move(localHits);
                *wireEventID = evt;
                *wiresVec = std::move(localWires);
                hitWriter->Fill();
                wireWriter->Fill();
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
