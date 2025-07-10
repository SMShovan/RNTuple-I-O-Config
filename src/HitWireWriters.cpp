#include "HitWireWriters.hpp"
#include "Hit.hpp"
#include "Wire.hpp"
#include "HitWireGenerators.hpp"
#include "Utils.hpp"
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>
#include <ROOT/RNTupleParallelWriter.hxx>
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

// Add at the top, after includes:
static int get_nthreads() {
    int n = std::thread::hardware_concurrency();
    return n > 0 ? n : 4;
}

// Central place for output directory
static const std::string kOutputDir = "./output";

void generateAndWrite_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");
 
    //--------------------------------------------------------------------
    // 1)  FIRST PASS  –  write the *hits* ntuple only (single HitVector column)
    //--------------------------------------------------------------------
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitVector>("HitVector");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
 
        int nThreads = get_nthreads();
        auto seeds    = Utils::generateSeeds(nThreads);
 
        auto fillHits = [&](int first, int last, unsigned seed) {
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
 
            auto hitObj = entry->GetPtr<HitVector>("HitVector");
 
            TStopwatch t; t.Reset();
            for (int i = first; i < last; ++i) {
                HitVector hv = generateRandomHitVector(i, hitsPerEvent, rng);
                t.Start();
                *hitObj = std::move(hv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };
 
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for (int th = 0; th < nThreads; ++th) {
            int begin = th * chunk;
            int end   = (th == nThreads - 1) ? numEvents : begin + chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double timeHits = 0.0;
        for (auto &f : futs) timeHits += f.get();
        std::cout << "[Seq] Hits ntuple written in " << timeHits * 1000 << " ms\n";
    }
 
    //--------------------------------------------------------------------
    // 2)  SECOND PASS –  write the *wires* ntuple only (full schema)
    //--------------------------------------------------------------------
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
 
        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);
 
        auto fillWires = [&](int first, int last, unsigned seed) {
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireVector>("WireVector");
            TStopwatch t; t.Reset();
            for (int i = first; i < last; ++i) {
                WireVector wv = generateRandomWireVector(i, wiresPerEvent, wiresPerEvent, rng);
                t.Start();
                *wireObj = std::move(wv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };
 
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for (int th = 0; th < nThreads; ++th) {
            int begin = th * chunk;
            int end   = (th == nThreads - 1) ? numEvents : begin + chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double timeWires = 0.0;
        for (auto &f : futs) timeWires += f.get();
        std::cout << "[Seq] Wires ntuple written in " << timeWires * 1000 << " ms\n";
    }
}

void generateAndWrite_VertiSplit_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");
 
    // ------------------------------------------------------------
    // 1) Hits pass – expanded hit schema
    // ------------------------------------------------------------
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<long long>("EventID");
        hitModel->MakeField<std::vector<unsigned int>>("Channel");
        hitModel->MakeField<std::vector<int>>("View");
        hitModel->MakeField<std::vector<int>>("StartTick");
        hitModel->MakeField<std::vector<int>>("EndTick");
        hitModel->MakeField<std::vector<float>>("PeakTime");
        hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
        hitModel->MakeField<std::vector<float>>("RMS");
        hitModel->MakeField<std::vector<float>>("PeakAmplitude");
        hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
        hitModel->MakeField<std::vector<float>>("ROISummedADC");
        hitModel->MakeField<std::vector<float>>("HitSummedADC");
        hitModel->MakeField<std::vector<float>>("Integral");
        hitModel->MakeField<std::vector<float>>("SigmaIntegral");
        hitModel->MakeField<std::vector<short int>>("Multiplicity");
        hitModel->MakeField<std::vector<short int>>("LocalIndex");
        hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
        hitModel->MakeField<std::vector<int>>("NDF");
        hitModel->MakeField<std::vector<int>>("SignalType");
        hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
        hitModel->MakeField<std::vector<int>>("WireID_TPC");
        hitModel->MakeField<std::vector<int>>("WireID_Plane");
        hitModel->MakeField<std::vector<int>>("WireID_Wire");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
 
        int nThreads = get_nthreads();
        auto seeds = Utils::generateSeeds(nThreads);
 
        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            // ptrs
            auto eventID = entry->GetPtr<long long>("EventID");
            auto ch = entry->GetPtr<std::vector<unsigned int>>("Channel");
            auto view = entry->GetPtr<std::vector<int>>("View");
            auto sTick = entry->GetPtr<std::vector<int>>("StartTick");
            auto eTick = entry->GetPtr<std::vector<int>>("EndTick");
            auto peak = entry->GetPtr<std::vector<float>>("PeakTime");
            auto sigmaPeak = entry->GetPtr<std::vector<float>>("SigmaPeakTime");
            auto rms = entry->GetPtr<std::vector<float>>("RMS");
            auto amp = entry->GetPtr<std::vector<float>>("PeakAmplitude");
            auto sigmaAmp = entry->GetPtr<std::vector<float>>("SigmaPeakAmplitude");
            auto roiADC = entry->GetPtr<std::vector<float>>("ROISummedADC");
            auto hitADC = entry->GetPtr<std::vector<float>>("HitSummedADC");
            auto integ = entry->GetPtr<std::vector<float>>("Integral");
            auto sigmaInt = entry->GetPtr<std::vector<float>>("SigmaIntegral");
            auto mult = entry->GetPtr<std::vector<short int>>("Multiplicity");
            auto locIdx = entry->GetPtr<std::vector<short int>>("LocalIndex");
            auto gof = entry->GetPtr<std::vector<float>>("GoodnessOfFit");
            auto ndf = entry->GetPtr<std::vector<int>>("NDF");
            auto sigType = entry->GetPtr<std::vector<int>>("SignalType");
            auto cryo = entry->GetPtr<std::vector<int>>("WireID_Cryostat");
            auto tpc = entry->GetPtr<std::vector<int>>("WireID_TPC");
            auto plane = entry->GetPtr<std::vector<int>>("WireID_Plane");
            auto wire = entry->GetPtr<std::vector<int>>("WireID_Wire");
 
            TStopwatch sw; sw.Reset();
            for(int evt=first; evt<last; ++evt){
                HitVector hv = generateRandomHitVector(evt, hitsPerEvent, rng);
                sw.Start();
                *eventID = hv.EventID;
                *ch = std::move(hv.getChannel());
                *view = std::move(hv.getView());
                *sTick = std::move(hv.getStartTick());
                *eTick = std::move(hv.getEndTick());
                *peak = std::move(hv.getPeakTime());
                *sigmaPeak = std::move(hv.getSigmaPeakTime());
                *rms = std::move(hv.getRMS());
                *amp = std::move(hv.getPeakAmplitude());
                *sigmaAmp = std::move(hv.getSigmaPeakAmplitude());
                *roiADC = std::move(hv.getROISummedADC());
                *hitADC = std::move(hv.getHitSummedADC());
                *integ = std::move(hv.getIntegral());
                *sigmaInt = std::move(hv.getSigmaIntegral());
                *mult = std::move(hv.getMultiplicity());
                *locIdx = std::move(hv.getLocalIndex());
                *gof = std::move(hv.getGoodnessOfFit());
                *ndf = std::move(hv.getNDF());
                *sigType = std::move(hv.getSignalType());
                *cryo = std::move(hv.getWireID_Cryostat());
                *tpc = std::move(hv.getWireID_TPC());
                *plane = std::move(hv.getWireID_Plane());
                *wire = std::move(hv.getWireID_Wire());
                ctx->Fill(*entry);
                sw.Stop();
            }
            return sw.RealTime();
        };
 
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits+=f.get();
        std::cout << "[Seq] VertiSplit-Vector Hits written in "<<tHits*1000<<" ms\n";
    }
 
    // ------------------------------------------------------------
    // 2) Wires pass – WireVector column
    // ------------------------------------------------------------
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
 
        int nThreads = get_nthreads();
        auto seeds = Utils::generateSeeds(nThreads);
 
        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireVector>("WireVector");
            TStopwatch t; t.Reset();
            for (int i = first; i < last; ++i) {
                WireVector wv = generateRandomWireVector(i, wiresPerEvent, wiresPerEvent, rng);
                t.Start();
                *wireObj = std::move(wv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };
 
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires+=f.get();
        std::cout << "[Seq] VertiSplit-Vector Wires written in "<<tWires*1000<<" ms\n";
    }
}

void generateAndWrite_HoriSpill_Hit_Wire_Vector(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills; // keep total wires constant across spils
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");
 
    //------------------------------------------------------------------
    // 1) Hits ntuple (expanded vector schema)
    //------------------------------------------------------------------
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<long long>("EventID");
        hitModel->MakeField<int>("SpilID");
        hitModel->MakeField<std::vector<unsigned int>>("Channel");
        hitModel->MakeField<std::vector<int>>("View");
        hitModel->MakeField<std::vector<int>>("StartTick");
        hitModel->MakeField<std::vector<int>>("EndTick");
        hitModel->MakeField<std::vector<float>>("PeakTime");
        hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
        hitModel->MakeField<std::vector<float>>("RMS");
        hitModel->MakeField<std::vector<float>>("PeakAmplitude");
        hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
        hitModel->MakeField<std::vector<float>>("ROISummedADC");
        hitModel->MakeField<std::vector<float>>("HitSummedADC");
        hitModel->MakeField<std::vector<float>>("Integral");
        hitModel->MakeField<std::vector<float>>("SigmaIntegral");
        hitModel->MakeField<std::vector<short int>>("Multiplicity");
        hitModel->MakeField<std::vector<short int>>("LocalIndex");
        hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
        hitModel->MakeField<std::vector<int>>("NDF");
        hitModel->MakeField<std::vector<int>>("SignalType");
        hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
        hitModel->MakeField<std::vector<int>>("WireID_TPC");
        hitModel->MakeField<std::vector<int>>("WireID_Plane");
        hitModel->MakeField<std::vector<int>>("WireID_Wire");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
 
        int total = numEvents * numHoriSpills;
        int nThreads = get_nthreads();
        auto seeds = Utils::generateSeeds(nThreads);
 
        auto fillHits = [&](int start, int end, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
 
            auto eventID = entry->GetPtr<long long>("EventID");
            auto spilID  = entry->GetPtr<int>("SpilID");
            auto ch   = entry->GetPtr<std::vector<unsigned int>>("Channel");
            auto view = entry->GetPtr<std::vector<int>>("View");
            auto sTick= entry->GetPtr<std::vector<int>>("StartTick");
            auto eTick= entry->GetPtr<std::vector<int>>("EndTick");
            auto peak = entry->GetPtr<std::vector<float>>("PeakTime");
            auto sigmaPeak = entry->GetPtr<std::vector<float>>("SigmaPeakTime");
            auto rms  = entry->GetPtr<std::vector<float>>("RMS");
            auto amp  = entry->GetPtr<std::vector<float>>("PeakAmplitude");
            auto sigmaAmp = entry->GetPtr<std::vector<float>>("SigmaPeakAmplitude");
            auto roiADC   = entry->GetPtr<std::vector<float>>("ROISummedADC");
            auto hitADC   = entry->GetPtr<std::vector<float>>("HitSummedADC");
            auto integ    = entry->GetPtr<std::vector<float>>("Integral");
            auto sigmaInt = entry->GetPtr<std::vector<float>>("SigmaIntegral");
            auto mult     = entry->GetPtr<std::vector<short int>>("Multiplicity");
            auto locIdx   = entry->GetPtr<std::vector<short int>>("LocalIndex");
            auto gof      = entry->GetPtr<std::vector<float>>("GoodnessOfFit");
            auto ndf      = entry->GetPtr<std::vector<int>>("NDF");
            auto sigType  = entry->GetPtr<std::vector<int>>("SignalType");
            auto cryo     = entry->GetPtr<std::vector<int>>("WireID_Cryostat");
            auto tpc      = entry->GetPtr<std::vector<int>>("WireID_TPC");
            auto plane    = entry->GetPtr<std::vector<int>>("WireID_Plane");
            auto wireV    = entry->GetPtr<std::vector<int>>("WireID_Wire");
 
            TStopwatch sw; sw.Reset();
            for(int idx=start; idx<end; ++idx){
                int evt = idx / numHoriSpills;
                int spil = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;
                HitVector hv = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
 
                sw.Start();
                *eventID = hv.EventID;
                *spilID = spil;
                *ch = std::move(hv.getChannel());
                *view = std::move(hv.getView());
                *sTick = std::move(hv.getStartTick());
                *eTick = std::move(hv.getEndTick());
                *peak = std::move(hv.getPeakTime());
                *sigmaPeak = std::move(hv.getSigmaPeakTime());
                *rms = std::move(hv.getRMS());
                *amp = std::move(hv.getPeakAmplitude());
                *sigmaAmp = std::move(hv.getSigmaPeakAmplitude());
                *roiADC = std::move(hv.getROISummedADC());
                *hitADC = std::move(hv.getHitSummedADC());
                *integ = std::move(hv.getIntegral());
                *sigmaInt = std::move(hv.getSigmaIntegral());
                *mult = std::move(hv.getMultiplicity());
                *locIdx = std::move(hv.getLocalIndex());
                *gof = std::move(hv.getGoodnessOfFit());
                *ndf = std::move(hv.getNDF());
                *sigType = std::move(hv.getSignalType());
                *cryo = std::move(hv.getWireID_Cryostat());
                *tpc = std::move(hv.getWireID_TPC());
                *plane = std::move(hv.getWireID_Plane());
                *wireV = std::move(hv.getWireID_Wire());
                ctx->Fill(*entry);
                sw.Stop();
            }
            return sw.RealTime();
        };
 
        int chunk = (numEvents*numHoriSpills) / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end = (th==nThreads-1)?(numEvents*numHoriSpills):begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits+=f.get();
        std::cout << "[Seq] VertiSpil-Vector Hits written in "<<tHits*1000<<" ms\n";
    }
 
    //------------------------------------------------------------------
    // 2) Wires ntuple pass – WireVector column
    //------------------------------------------------------------------
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
 
        int total = numEvents * numHoriSpills;
        int nThreads = get_nthreads();
        auto seeds = Utils::generateSeeds(nThreads);
 
        auto fillWires = [&](int start, int end, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireVector>("WireVector");
            TStopwatch sw; sw.Reset();
            for(int idx=start; idx<end; ++idx){
                int evt = idx / numHoriSpills;
                int spil = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;
                WireVector wv = generateRandomWireVector(uid, adjustedWiresPerEvent, wiresPerEvent, rng);
                sw.Start();
                *wireObj = std::move(wv);
                ctx->Fill(*entry);
                sw.Stop();
            }
            return sw.RealTime();
        };
 
        int chunk = total / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end = (th==nThreads-1)?total:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires+=f.get();
        std::cout << "[Seq] VertiSpil-Vector Wires written in "<<tWires*1000<<" ms\n";
    }
}

void generateAndWrite_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<long long>("EventID");
        hitModel->MakeField<unsigned int>("Channel");
        hitModel->MakeField<int>("View");
        hitModel->MakeField<int>("StartTick");
        hitModel->MakeField<int>("EndTick");
        hitModel->MakeField<float>("PeakTime");
        hitModel->MakeField<float>("SigmaPeakTime");
        hitModel->MakeField<float>("RMS");
        hitModel->MakeField<float>("PeakAmplitude");
        hitModel->MakeField<float>("SigmaPeakAmplitude");
        hitModel->MakeField<float>("ROISummedADC");
        hitModel->MakeField<float>("HitSummedADC");
        hitModel->MakeField<float>("Integral");
        hitModel->MakeField<float>("SigmaIntegral");
        hitModel->MakeField<short int>("Multiplicity");
        hitModel->MakeField<short int>("LocalIndex");
        hitModel->MakeField<float>("GoodnessOfFit");
        hitModel->MakeField<int>("NDF");
        hitModel->MakeField<int>("SignalType");
        hitModel->MakeField<int>("WireID_Cryostat");
        hitModel->MakeField<int>("WireID_TPC");
        hitModel->MakeField<int>("WireID_Plane");
        hitModel->MakeField<int>("WireID_Wire");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        // --- WIRE NTUPLE MODEL for Individual-based writers ---
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        
        // Use only one thread for this experiment
        int nThreads = get_nthreads();
        auto seeds = Utils::generateSeeds(nThreads);
        
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            
            // Per-thread timer for RNTuple storage only
            TStopwatch storageTimer;
            storageTimer.Reset();
            
            auto eventID = hitEntry->GetPtr<long long>("EventID");
            auto fChannel = hitEntry->GetPtr<unsigned int>("Channel");
            auto fView = hitEntry->GetPtr<int>("View");
            auto fStartTick = hitEntry->GetPtr<int>("StartTick");
            auto fEndTick = hitEntry->GetPtr<int>("EndTick");
            auto fPeakTime = hitEntry->GetPtr<float>("PeakTime");
            auto fSigmaPeakTime = hitEntry->GetPtr<float>("SigmaPeakTime");
            auto fRMS = hitEntry->GetPtr<float>("RMS");
            auto fPeakAmplitude = hitEntry->GetPtr<float>("PeakAmplitude");
            auto fSigmaPeakAmplitude = hitEntry->GetPtr<float>("SigmaPeakAmplitude");
            auto fROISummedADC = hitEntry->GetPtr<float>("ROISummedADC");
            auto fHitSummedADC = hitEntry->GetPtr<float>("HitSummedADC");
            auto fIntegral = hitEntry->GetPtr<float>("Integral");
            auto fSigmaIntegral = hitEntry->GetPtr<float>("SigmaIntegral");
            auto fMultiplicity = hitEntry->GetPtr<short int>("Multiplicity");
            auto fLocalIndex = hitEntry->GetPtr<short int>("LocalIndex");
            auto fGoodnessOfFit = hitEntry->GetPtr<float>("GoodnessOfFit");
            auto fNDF = hitEntry->GetPtr<int>("NDF");
            auto fSignalType = hitEntry->GetPtr<int>("SignalType");
            auto fWireID_Cryostat = hitEntry->GetPtr<int>("WireID_Cryostat");
            auto fWireID_TPC = hitEntry->GetPtr<int>("WireID_TPC");
            auto fWireID_Plane = hitEntry->GetPtr<int>("WireID_Plane");
            auto fWireID_Wire = hitEntry->GetPtr<int>("WireID_Wire");

            auto wireObj = wireEntry->GetPtr<WireIndividual>("WireIndividual");
            
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                // Generate multiple hits and wires for this event to match Vector data volume
                for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
                    // Generate data (not timed)
                    HitIndividual hit = generateRandomHitIndividual(eventIndex, rng);
                    WireIndividual wire = generateRandomWireIndividual(eventIndex, wiresPerEvent, rng);
                    
                    // TIMING: RNTuple Storage Operations only
                    storageTimer.Start();
                    *eventID = hit.EventID;
                    *fChannel = hit.fChannel;
                    *fView = hit.fView;
                    *fStartTick = hit.fStartTick;
                    *fEndTick = hit.fEndTick;
                    *fPeakTime = hit.fPeakTime;
                    *fSigmaPeakTime = hit.fSigmaPeakTime;
                    *fRMS = hit.fRMS;
                    *fPeakAmplitude = hit.fPeakAmplitude;
                    *fSigmaPeakAmplitude = hit.fSigmaPeakAmplitude;
                    *fROISummedADC = hit.fROISummedADC;
                    *fHitSummedADC = hit.fHitSummedADC;
                    *fIntegral = hit.fIntegral;
                    *fSigmaIntegral = hit.fSigmaIntegral;
                    *fMultiplicity = hit.fMultiplicity;
                    *fLocalIndex = hit.fLocalIndex;
                    *fGoodnessOfFit = hit.fGoodnessOfFit;
                    *fNDF = hit.fNDF;
                    *fSignalType = hit.fSignalType;
                    *fWireID_Cryostat = hit.fWireID_Cryostat;
                    *fWireID_TPC = hit.fWireID_TPC;
                    *fWireID_Plane = hit.fWireID_Plane;
                    *fWireID_Wire = hit.fWireID_Wire;

                    *wireObj = wire;
                    
                    hitFillContext->Fill(*hitEntry);
                    wireFillContext->Fill(*wireEntry);
                    storageTimer.Stop();
                }
            }
            
            // Return only storage timing
            return storageTimer.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futures;
        
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        
        // Collect storage timing results from all threads
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        
        std::cout << "generateAndWriteHitWireDataIndividual:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

void generateAndWrite_VertiSplit_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    //----------------------------------------------------
    // 1) Hits pass – per-hit scalar columns
    //----------------------------------------------------
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<long long>("EventID");
        hitModel->MakeField<unsigned int>("Channel");
        hitModel->MakeField<int>("View");
        hitModel->MakeField<int>("StartTick");
        hitModel->MakeField<int>("EndTick");
        hitModel->MakeField<float>("PeakTime");
        hitModel->MakeField<float>("SigmaPeakTime");
        hitModel->MakeField<float>("RMS");
        hitModel->MakeField<float>("PeakAmplitude");
        hitModel->MakeField<float>("SigmaPeakAmplitude");
        hitModel->MakeField<float>("ROISummedADC");
        hitModel->MakeField<float>("HitSummedADC");
        hitModel->MakeField<float>("Integral");
        hitModel->MakeField<float>("SigmaIntegral");
        hitModel->MakeField<short int>("Multiplicity");
        hitModel->MakeField<short int>("LocalIndex");
        hitModel->MakeField<float>("GoodnessOfFit");
        hitModel->MakeField<int>("NDF");
        hitModel->MakeField<int>("SignalType");
        hitModel->MakeField<int>("WireID_Cryostat");
        hitModel->MakeField<int>("WireID_TPC");
        hitModel->MakeField<int>("WireID_Plane");
        hitModel->MakeField<int>("WireID_Wire");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int beginEvt, int endEvt, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();

            // ptrs
            auto eventID = entry->GetPtr<long long>("EventID");
            auto ch = entry->GetPtr<unsigned int>("Channel");
            auto view = entry->GetPtr<int>("View");
            auto sTick = entry->GetPtr<int>("StartTick");
            auto eTick = entry->GetPtr<int>("EndTick");
            auto peak = entry->GetPtr<float>("PeakTime");
            auto sigmaPeak = entry->GetPtr<float>("SigmaPeakTime");
            auto rms = entry->GetPtr<float>("RMS");
            auto amp = entry->GetPtr<float>("PeakAmplitude");
            auto sigmaAmp = entry->GetPtr<float>("SigmaPeakAmplitude");
            auto roiADC = entry->GetPtr<float>("ROISummedADC");
            auto hitADC = entry->GetPtr<float>("HitSummedADC");
            auto integ = entry->GetPtr<float>("Integral");
            auto sigmaInt = entry->GetPtr<float>("SigmaIntegral");
            auto mult = entry->GetPtr<short int>("Multiplicity");
            auto locIdx = entry->GetPtr<short int>("LocalIndex");
            auto gof = entry->GetPtr<float>("GoodnessOfFit");
            auto ndf = entry->GetPtr<int>("NDF");
            auto sigType = entry->GetPtr<int>("SignalType");
            auto cryo = entry->GetPtr<int>("WireID_Cryostat");
            auto tpc = entry->GetPtr<int>("WireID_TPC");
            auto plane = entry->GetPtr<int>("WireID_Plane");
            auto wire = entry->GetPtr<int>("WireID_Wire");

            TStopwatch sw; sw.Reset();
            for(int evt=beginEvt; evt<endEvt; ++evt){
                for(int h=0; h<hitsPerEvent; ++h){
                    HitIndividual hit = generateRandomHitIndividual(evt, rng);
                    sw.Start();
                    *eventID = hit.EventID;
                    *ch = hit.fChannel;
                    *view = hit.fView;
                    *sTick = hit.fStartTick;
                    *eTick = hit.fEndTick;
                    *peak = hit.fPeakTime;
                    *sigmaPeak = hit.fSigmaPeakTime;
                    *rms = hit.fRMS;
                    *amp = hit.fPeakAmplitude;
                    *sigmaAmp = hit.fSigmaPeakAmplitude;
                    *roiADC = hit.fROISummedADC;
                    *hitADC = hit.fHitSummedADC;
                    *integ = hit.fIntegral;
                    *sigmaInt = hit.fSigmaIntegral;
                    *mult = hit.fMultiplicity;
                    *locIdx = hit.fLocalIndex;
                    *gof = hit.fGoodnessOfFit;
                    *ndf = hit.fNDF;
                    *sigType = hit.fSignalType;
                    *cryo = hit.fWireID_Cryostat;
                    *tpc = hit.fWireID_TPC;
                    *plane = hit.fWireID_Plane;
                    *wire = hit.fWireID_Wire;
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits+=f.get();
        std::cout << "[Seq] HoriSplit-Individual Hits written in "<<tHits*1000<<" ms\n";
    }

    //----------------------------------------------------
    // 2) Wires pass – WireIndividual column
    //----------------------------------------------------
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int beginEvt, int endEvt, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireIndividual>("WireIndividual");
            TStopwatch sw; sw.Reset();
            for(int evt=beginEvt; evt<endEvt; ++evt){
                for(int w=0; w<wiresPerEvent; ++w){
                    WireIndividual wi = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                    sw.Start();
                    *wireObj = std::move(wi);
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires+=f.get();
        std::cout << "[Seq] HoriSplit-Individual Wires written in "<<tWires*1000<<" ms\n";
    }
}

void generateAndWrite_HoriSpill_Hit_Wire_Individual(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent   = hitsPerEvent   / numHoriSpills;
    int adjustedWiresPerEvent  = wiresPerEvent  / numHoriSpills; // keep total wires constant across spils

    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    // ------------------------------------------------------------
    // 1)  Hits pass – scalar-per-hit schema + SpilID
    // ------------------------------------------------------------
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<long long>("EventID");
        hitModel->MakeField<int>("SpilID");
        hitModel->MakeField<unsigned int>("Channel");
        hitModel->MakeField<int>("View");
        hitModel->MakeField<int>("StartTick");
        hitModel->MakeField<int>("EndTick");
        hitModel->MakeField<float>("PeakTime");
        hitModel->MakeField<float>("SigmaPeakTime");
        hitModel->MakeField<float>("RMS");
        hitModel->MakeField<float>("PeakAmplitude");
        hitModel->MakeField<float>("SigmaPeakAmplitude");
        hitModel->MakeField<float>("ROISummedADC");
        hitModel->MakeField<float>("HitSummedADC");
        hitModel->MakeField<float>("Integral");
        hitModel->MakeField<float>("SigmaIntegral");
        hitModel->MakeField<short int>("Multiplicity");
        hitModel->MakeField<short int>("LocalIndex");
        hitModel->MakeField<float>("GoodnessOfFit");
        hitModel->MakeField<int>("NDF");
        hitModel->MakeField<int>("SignalType");
        hitModel->MakeField<int>("WireID_Cryostat");
        hitModel->MakeField<int>("WireID_TPC");
        hitModel->MakeField<int>("WireID_Plane");
        hitModel->MakeField<int>("WireID_Wire");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int totalEntries = numEvents * numHoriSpills;          // each entry represents one "event,spil" pair
        int nThreads     = get_nthreads();
        auto seeds       = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();

            // Pointers
            auto eventID = entry->GetPtr<long long>("EventID");
            auto spilID  = entry->GetPtr<int>("SpilID");
            auto ch      = entry->GetPtr<unsigned int>("Channel");
            auto view    = entry->GetPtr<int>("View");
            auto sTick   = entry->GetPtr<int>("StartTick");
            auto eTick   = entry->GetPtr<int>("EndTick");
            auto peak    = entry->GetPtr<float>("PeakTime");
            auto sigmaPk = entry->GetPtr<float>("SigmaPeakTime");
            auto rms     = entry->GetPtr<float>("RMS");
            auto amp     = entry->GetPtr<float>("PeakAmplitude");
            auto sigmaAmp= entry->GetPtr<float>("SigmaPeakAmplitude");
            auto roiADC  = entry->GetPtr<float>("ROISummedADC");
            auto hitADC  = entry->GetPtr<float>("HitSummedADC");
            auto integ   = entry->GetPtr<float>("Integral");
            auto sigmaInt= entry->GetPtr<float>("SigmaIntegral");
            auto mult    = entry->GetPtr<short int>("Multiplicity");
            auto locIdx  = entry->GetPtr<short int>("LocalIndex");
            auto gof     = entry->GetPtr<float>("GoodnessOfFit");
            auto ndf     = entry->GetPtr<int>("NDF");
            auto sigType = entry->GetPtr<int>("SignalType");
            auto cryo    = entry->GetPtr<int>("WireID_Cryostat");
            auto tpc     = entry->GetPtr<int>("WireID_TPC");
            auto plane   = entry->GetPtr<int>("WireID_Plane");
            auto wire    = entry->GetPtr<int>("WireID_Wire");

            TStopwatch sw; sw.Reset();
            for(int idx=first; idx<last; ++idx){
                int evt   = idx / numHoriSpills;
                int spil  = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;

                for(int h=0; h<adjustedHitsPerEvent; ++h){
                    HitIndividual hit = generateRandomHitIndividual(uid, rng);

                    sw.Start();
                    *eventID  = hit.EventID;
                    *spilID   = spil;
                    *ch       = hit.fChannel;
                    *view     = hit.fView;
                    *sTick    = hit.fStartTick;
                    *eTick    = hit.fEndTick;
                    *peak     = hit.fPeakTime;
                    *sigmaPk  = hit.fSigmaPeakTime;
                    *rms      = hit.fRMS;
                    *amp      = hit.fPeakAmplitude;
                    *sigmaAmp = hit.fSigmaPeakAmplitude;
                    *roiADC   = hit.fROISummedADC;
                    *hitADC   = hit.fHitSummedADC;
                    *integ    = hit.fIntegral;
                    *sigmaInt = hit.fSigmaIntegral;
                    *mult     = hit.fMultiplicity;
                    *locIdx   = hit.fLocalIndex;
                    *gof      = hit.fGoodnessOfFit;
                    *ndf      = hit.fNDF;
                    *sigType  = hit.fSignalType;
                    *cryo     = hit.fWireID_Cryostat;
                    *tpc      = hit.fWireID_TPC;
                    *plane    = hit.fWireID_Plane;
                    *wire     = hit.fWireID_Wire;
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = totalEntries / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1) ? totalEntries : begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] VertiSpil-Individual Hits written in " << tHits*1000 << " ms\n";
    }

    // ------------------------------------------------------------
    // 2)  Wires pass – single WireIndividual column
    // ------------------------------------------------------------
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int totalEntries = numEvents * numHoriSpills;
        int nThreads     = get_nthreads();
        auto seeds       = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireIndividual>("WireIndividual");

            TStopwatch sw; sw.Reset();
            for(int idx=first; idx<last; ++idx){
                int evt   = idx / numHoriSpills;
                int spil  = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;
                for(int w=0; w<adjustedWiresPerEvent; ++w){
                    WireIndividual wi = generateRandomWireIndividual(uid, wiresPerEvent, rng);
                    sw.Start();
                    *wireObj = std::move(wi);
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = totalEntries / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1) ? totalEntries : begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] VertiSpil-Individual Wires written in " << tWires*1000 << " ms\n";
    }
}

// --- New: Store entire HitVector and WireVector as single fields (Vector-based) ---
void generateAndWrite_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    // Sequential version: first write HitVector column, then WireVector column
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    //--------------------------------------------------
    // 1) Hits pass – single HitVector column
    //--------------------------------------------------
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitVector>("HitVector");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto hitObj = entry->GetPtr<HitVector>("HitVector");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                HitVector hv = generateRandomHitVector(evt, hitsPerEvent, rng);
                t.Start();
                *hitObj = std::move(hv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] Vector-Dict Hits written in "<<tHits*1000<<" ms\n";
    }

    //--------------------------------------------------
    // 2) Wires pass – single WireVector column
    //--------------------------------------------------
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireVector>("WireVector");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                WireVector wv = generateRandomWireVector(evt, wiresPerEvent, rng);
                t.Start();
                *wireObj = std::move(wv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] Vector-Dict Wires written in "<<tWires*1000<<" ms\n";
    }
}

// --- New: Store entire HitIndividual and WireIndividual as single fields (Individual-based) ---
void generateAndWrite_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    // -----------------------------
    // 1) Hits pass – HitIndividual
    // -----------------------------
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitIndividual>("HitIndividual");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto hitObj = entry->GetPtr<HitIndividual>("HitIndividual");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                for(int h=0; h<hitsPerEvent; ++h){
                    HitIndividual hit = generateRandomHitIndividual(evt, rng);
                    t.Start();
                    *hitObj = std::move(hit);
                    ctx->Fill(*entry);
                    t.Stop();
                }
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] Individual-Dict Hits written in "<<tHits*1000<<" ms\n";
    }

    // -------------------------------
    // 2) Wires pass – WireIndividual
    // -------------------------------
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireIndividual>("WireIndividual");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                for(int w=0; w<wiresPerEvent; ++w){
                    WireIndividual wi = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                    t.Start();
                    *wireObj = std::move(wi);
                    ctx->Fill(*entry);
                    t.Stop();
                }
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] Individual-Dict Wires written in "<<tWires*1000<<" ms\n";
    }
}

// --- New: Split Vector-based, store as objects ---
void generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    // This variant stores the entire HitVector & WireVector objects, mirroring the reader expectation.
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    // 1) Hits pass – HitVector column
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitVector>("HitVector");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto hitObj = entry->GetPtr<HitVector>("HitVector");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                HitVector hv = generateRandomHitVector(evt, hitsPerEvent, rng);
                t.Start();
                *hitObj = std::move(hv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] VertiSplit-Vector-Dict Hits written in "<<tHits*1000<<" ms\n";
    }

    // 2) Wires pass – WireVector column
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireVector>("WireVector");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                WireVector wv = generateRandomWireVector(evt, wiresPerEvent, wiresPerEvent, rng);
                t.Start();
                *wireObj = std::move(wv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] VertiSplit-Vector-Dict Wires written in "<<tWires*1000<<" ms\n";
    }
}

// --- New: Split Individual-based, store as objects ---
void generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    // 1) Hits pass – HitIndividual per entry
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitIndividual>("HitIndividual");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto hitObj = entry->GetPtr<HitIndividual>("HitIndividual");
            TStopwatch sw; sw.Reset();
            for(int evt=first; evt<last; ++evt){
                for(int h=0; h<hitsPerEvent; ++h){
                    HitIndividual hit = generateRandomHitIndividual(evt, rng);
                    sw.Start();
                    *hitObj = std::move(hit);
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] HoriSplit-Individual-Dict Hits written in "<<tHits*1000<<" ms\n";
    }

    // 2) Wires pass – WireIndividual per entry
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireIndividual>("WireIndividual");
            TStopwatch sw; sw.Reset();
            for(int evt=first; evt<last; ++evt){
                for(int w=0; w<wiresPerEvent; ++w){
                    WireIndividual wi = generateRandomWireIndividual(evt, wiresPerEvent, rng);
                    sw.Start();
                    *wireObj = std::move(wi);
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] HoriSplit-Individual-Dict Wires written in "<<tWires*1000<<" ms\n";
    }
}

// --- New: Spil Vector-based, store as objects ---
void generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    // Spil variant storing full HitVector/WireVector objects.
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent  = hitsPerEvent  / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;

    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    int totalEntries = numEvents * numHoriSpills;

    // 1) Hits pass
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitVector>("HitVector");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto hitObj = entry->GetPtr<HitVector>("HitVector");
            TStopwatch t; t.Reset();
            for(int idx=first; idx<last; ++idx){
                int evt  = idx / numHoriSpills;
                int spil = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;
                HitVector hv = generateRandomHitVector(uid, adjustedHitsPerEvent, rng);
                t.Start();
                *hitObj = std::move(hv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = totalEntries / get_nthreads();
        std::vector<std::future<double>> futs;
        for(int th=0; th<get_nthreads(); ++th){
            int begin = th*chunk;
            int end   = (th==get_nthreads()-1)?totalEntries:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] VertiSpil-Vector-Dict Hits written in "<<tHits*1000<<" ms\n";
    }

    // 2) Wires pass
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireVector>("WireVector");
            TStopwatch t; t.Reset();
            for(int idx=first; idx<last; ++idx){
                int evt  = idx / numHoriSpills;
                int spil = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;
                WireVector wv = generateRandomWireVector(uid, adjustedWiresPerEvent, wiresPerEvent, rng);
                t.Start();
                *wireObj = std::move(wv);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = totalEntries / get_nthreads();
        std::vector<std::future<double>> futs;
        for(int th=0; th<get_nthreads(); ++th){
            int begin = th*chunk;
            int end   = (th==get_nthreads()-1)?totalEntries:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] VertiSpil-Vector-Dict Wires written in "<<tWires*1000<<" ms\n";
    }
}

// --- New: Spil Individual-based, store as objects ---
void generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent  = hitsPerEvent  / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    int totalEntries = numEvents * numHoriSpills;

    // 1) Hits pass – HitIndividual
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitIndividual>("HitIndividual");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto hitObj = entry->GetPtr<HitIndividual>("HitIndividual");
            TStopwatch sw; sw.Reset();
            for(int idx=first; idx<last; ++idx){
                int evt  = idx / numHoriSpills;
                int spil = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;
                for(int h=0; h<adjustedHitsPerEvent; ++h){
                    HitIndividual hit = generateRandomHitIndividual(uid, rng);
                    sw.Start();
                    *hitObj = std::move(hit);
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = totalEntries / get_nthreads();
        std::vector<std::future<double>> futs;
        for(int th=0; th<get_nthreads(); ++th){
            int begin = th*chunk;
            int end   = (th==get_nthreads()-1)?totalEntries:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] VertiSpil-Individual-Dict Hits written in "<<tHits*1000<<" ms\n";
    }

    // 2) Wires pass – WireIndividual
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto wireObj = entry->GetPtr<WireIndividual>("WireIndividual");
            TStopwatch sw; sw.Reset();
            for(int idx=first; idx<last; ++idx){
                int evt  = idx / numHoriSpills;
                int spil = idx % numHoriSpills;
                long long uid = static_cast<long long>(evt)*10000 + spil;
                for(int w=0; w<adjustedWiresPerEvent; ++w){
                    WireIndividual wi = generateRandomWireIndividual(uid, wiresPerEvent, rng);
                    sw.Start();
                    *wireObj = std::move(wi);
                    ctx->Fill(*entry);
                    sw.Stop();
                }
            }
            return sw.RealTime();
        };

        int chunk = totalEntries / get_nthreads();
        std::vector<std::future<double>> futs;
        for(int th=0; th<get_nthreads(); ++th){
            int begin = th*chunk;
            int end   = (th==get_nthreads()-1)?totalEntries:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] VertiSpil-Individual-Dict Wires written in "<<tWires*1000<<" ms\n";
    }
}

// --- Vector-of-Individuals (sequential passes) ---
void generateAndWrite_Hit_Wire_Vector_Of_Individuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories(kOutputDir);
    TFile file(fileName.c_str(), "RECREATE");

    // 1) Hits vector pass
    {
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<long long>("EventID");
        hitModel->MakeField<std::vector<HitIndividual>>("Hits");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillHits = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = hitWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto eventID = entry->GetPtr<long long>("EventID");
            auto hitsVec = entry->GetPtr<std::vector<HitIndividual>>("Hits");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                std::vector<HitIndividual> hits; hits.reserve(hitsPerEvent);
                for(int i=0;i<hitsPerEvent;++i) hits.push_back(generateRandomHitIndividual(evt,rng));
                t.Start();
                *eventID = evt;
                *hitsVec = std::move(hits);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillHits, begin, end, seeds[th]));
        }
        double tHits=0; for(auto &f:futs) tHits += f.get();
        std::cout << "[Seq] Vector-of-Individuals Hits written in "<<tHits*1000<<" ms\n";
    }

    // 2) Wires vector pass
    {
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<std::vector<WireIndividual>>("Wires");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = get_nthreads();
        auto seeds   = Utils::generateSeeds(nThreads);

        auto fillWires = [&](int first, int last, unsigned seed){
            std::mt19937 rng(seed);
            auto ctx   = wireWriter->CreateFillContext();
            auto entry = ctx->CreateEntry();
            auto eventID = entry->GetPtr<long long>("EventID");
            auto wiresVec = entry->GetPtr<std::vector<WireIndividual>>("Wires");
            TStopwatch t; t.Reset();
            for(int evt=first; evt<last; ++evt){
                std::vector<WireIndividual> wires; wires.reserve(wiresPerEvent);
                for(int i=0;i<wiresPerEvent;++i){
                    wires.push_back(generateRandomWireIndividual(evt, wiresPerEvent, rng));
                }
                t.Start();
                *eventID = evt;
                *wiresVec = std::move(wires);
                ctx->Fill(*entry);
                t.Stop();
            }
            return t.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futs;
        for(int th=0; th<nThreads; ++th){
            int begin = th*chunk;
            int end   = (th==nThreads-1)?numEvents:begin+chunk;
            futs.emplace_back(std::async(std::launch::async, fillWires, begin, end, seeds[th]));
        }
        double tWires=0; for(auto &f:futs) tWires += f.get();
        std::cout << "[Seq] Vector-of-Individuals Wires written in "<<tWires*1000<<" ms\n";
    }
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
