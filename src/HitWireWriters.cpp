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

void generateAndWriteHitWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
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
        // --- WIRE NTUPLE MODEL for Vector-based writers ---
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<std::vector<unsigned int>>("fWire_Channel");
        wireModel->MakeField<std::vector<int>>("fWire_View");
        wireModel->MakeField<std::vector<unsigned int>>("fSignalROI_nROIs");
        wireModel->MakeField<std::vector<std::size_t>>("fSignalROI_offsets");
        wireModel->MakeField<std::vector<float>>("fSignalROI_data");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        
        // --- Parallel Fill Function ---
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            
            // Per-thread timer for RNTuple storage only
            TStopwatch storageTimer;
            storageTimer.Reset();
            
            // Per-thread, per-entry field pointers
            auto eventID = hitEntry->GetPtr<long long>("EventID");
            auto fChannel = hitEntry->GetPtr<std::vector<unsigned int>>("Channel");
            auto fView = hitEntry->GetPtr<std::vector<int>>("View");
            auto fStartTick = hitEntry->GetPtr<std::vector<int>>("StartTick");
            auto fEndTick = hitEntry->GetPtr<std::vector<int>>("EndTick");
            auto fPeakTime = hitEntry->GetPtr<std::vector<float>>("PeakTime");
            auto fSigmaPeakTime = hitEntry->GetPtr<std::vector<float>>("SigmaPeakTime");
            auto fRMS = hitEntry->GetPtr<std::vector<float>>("RMS");
            auto fPeakAmplitude = hitEntry->GetPtr<std::vector<float>>("PeakAmplitude");
            auto fSigmaPeakAmplitude = hitEntry->GetPtr<std::vector<float>>("SigmaPeakAmplitude");
            auto fROISummedADC = hitEntry->GetPtr<std::vector<float>>("ROISummedADC");
            auto fHitSummedADC = hitEntry->GetPtr<std::vector<float>>("HitSummedADC");
            auto fIntegral = hitEntry->GetPtr<std::vector<float>>("Integral");
            auto fSigmaIntegral = hitEntry->GetPtr<std::vector<float>>("SigmaIntegral");
            auto fMultiplicity = hitEntry->GetPtr<std::vector<short int>>("Multiplicity");
            auto fLocalIndex = hitEntry->GetPtr<std::vector<short int>>("LocalIndex");
            auto fGoodnessOfFit = hitEntry->GetPtr<std::vector<float>>("GoodnessOfFit");
            auto fNDF = hitEntry->GetPtr<std::vector<int>>("NDF");
            auto fSignalType = hitEntry->GetPtr<std::vector<int>>("SignalType");
            auto fWireID_Cryostat = hitEntry->GetPtr<std::vector<int>>("WireID_Cryostat");
            auto fWireID_TPC = hitEntry->GetPtr<std::vector<int>>("WireID_TPC");
            auto fWireID_Plane = hitEntry->GetPtr<std::vector<int>>("WireID_Plane");
            auto fWireID_Wire = hitEntry->GetPtr<std::vector<int>>("WireID_Wire");
            auto eventID_w = wireEntry->GetPtr<long long>("EventID");
            auto fWire_Channel_w = wireEntry->GetPtr<std::vector<unsigned int>>("fWire_Channel");
            auto fWire_View_w = wireEntry->GetPtr<std::vector<int>>("fWire_View");
            auto fSignalROI_nROIs_w = wireEntry->GetPtr<std::vector<unsigned int>>("fSignalROI_nROIs");
            auto fSignalROI_offsets_w = wireEntry->GetPtr<std::vector<std::size_t>>("fSignalROI_offsets");
            auto fSignalROI_data_w = wireEntry->GetPtr<std::vector<float>>("fSignalROI_data");
            
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                // Generate data (not timed)
                HitVector hit = std::move(generateRandomHitVector(eventIndex, hitsPerEvent, rng));
                WireVector wire = std::move(generateRandomWireVector(eventIndex, wiresPerEvent, wiresPerEvent, rng));
                
                // TIMING: RNTuple Storage Operations only
                storageTimer.Start();
                *eventID = hit.EventID;
                *fChannel = std::move(hit.getChannel());
                *fView = std::move(hit.getView());
                *fStartTick = std::move(hit.getStartTick());
                *fEndTick = std::move(hit.getEndTick());
                *fPeakTime = std::move(hit.getPeakTime());
                *fSigmaPeakTime = std::move(hit.getSigmaPeakTime());
                *fRMS = std::move(hit.getRMS());
                *fPeakAmplitude = std::move(hit.getPeakAmplitude());
                *fSigmaPeakAmplitude = std::move(hit.getSigmaPeakAmplitude());
                *fROISummedADC = std::move(hit.getROISummedADC());
                *fHitSummedADC = std::move(hit.getHitSummedADC());
                *fIntegral = std::move(hit.getIntegral());
                *fSigmaIntegral = std::move(hit.getSigmaIntegral());
                *fMultiplicity = std::move(hit.getMultiplicity());
                *fLocalIndex = std::move(hit.getLocalIndex());
                *fGoodnessOfFit = std::move(hit.getGoodnessOfFit());
                *fNDF = std::move(hit.getNDF());
                *fSignalType = std::move(hit.getSignalType());
                *fWireID_Cryostat = std::move(hit.getWireID_Cryostat());
                *fWireID_TPC = std::move(hit.getWireID_TPC());
                *fWireID_Plane = std::move(hit.getWireID_Plane());
                *fWireID_Wire = std::move(hit.getWireID_Wire());
                hitFillContext->Fill(*hitEntry);
                *eventID_w = wire.EventID;
                *fWire_Channel_w = wire.getWire_Channel();
                *fWire_View_w = wire.getWire_View();
                *fSignalROI_nROIs_w = wire.getSignalROI_nROIs();
                *fSignalROI_offsets_w = wire.getSignalROI_offsets();
                *fSignalROI_data_w = wire.getSignalROI_data();
                wireFillContext->Fill(*wireEntry);
                storageTimer.Stop();
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
        
        std::cout << "generateAndWriteHitWireDataVector:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    } // writers destroyed here, file still open
    // file destroyed here
}

void generateAndWriteSplitHitAndWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
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
        // --- WIRE NTUPLE MODEL for Vector-based writers ---
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<std::vector<unsigned int>>("fWire_Channel");
        wireModel->MakeField<std::vector<int>>("fWire_View");
        wireModel->MakeField<std::vector<unsigned int>>("fSignalROI_nROIs");
        wireModel->MakeField<std::vector<std::size_t>>("fSignalROI_offsets");
        wireModel->MakeField<std::vector<float>>("fSignalROI_data");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        
        // --- Parallel Fill Function ---
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            
            // Per-thread timer for RNTuple storage only
            TStopwatch storageTimer;
            storageTimer.Reset();
            
            // Per-thread, per-entry field pointers
            auto eventID = hitEntry->GetPtr<long long>("EventID");
            auto fChannel = hitEntry->GetPtr<std::vector<unsigned int>>("Channel");
            auto fView = hitEntry->GetPtr<std::vector<int>>("View");
            auto fStartTick = hitEntry->GetPtr<std::vector<int>>("StartTick");
            auto fEndTick = hitEntry->GetPtr<std::vector<int>>("EndTick");
            auto fPeakTime = hitEntry->GetPtr<std::vector<float>>("PeakTime");
            auto fSigmaPeakTime = hitEntry->GetPtr<std::vector<float>>("SigmaPeakTime");
            auto fRMS = hitEntry->GetPtr<std::vector<float>>("RMS");
            auto fPeakAmplitude = hitEntry->GetPtr<std::vector<float>>("PeakAmplitude");
            auto fSigmaPeakAmplitude = hitEntry->GetPtr<std::vector<float>>("SigmaPeakAmplitude");
            auto fROISummedADC = hitEntry->GetPtr<std::vector<float>>("ROISummedADC");
            auto fHitSummedADC = hitEntry->GetPtr<std::vector<float>>("HitSummedADC");
            auto fIntegral = hitEntry->GetPtr<std::vector<float>>("Integral");
            auto fSigmaIntegral = hitEntry->GetPtr<std::vector<float>>("SigmaIntegral");
            auto fMultiplicity = hitEntry->GetPtr<std::vector<short int>>("Multiplicity");
            auto fLocalIndex = hitEntry->GetPtr<std::vector<short int>>("LocalIndex");
            auto fGoodnessOfFit = hitEntry->GetPtr<std::vector<float>>("GoodnessOfFit");
            auto fNDF = hitEntry->GetPtr<std::vector<int>>("NDF");
            auto fSignalType = hitEntry->GetPtr<std::vector<int>>("SignalType");
            auto fWireID_Cryostat = hitEntry->GetPtr<std::vector<int>>("WireID_Cryostat");
            auto fWireID_TPC = hitEntry->GetPtr<std::vector<int>>("WireID_TPC");
            auto fWireID_Plane = hitEntry->GetPtr<std::vector<int>>("WireID_Plane");
            auto fWireID_Wire = hitEntry->GetPtr<std::vector<int>>("WireID_Wire");
            auto eventID_w = wireEntry->GetPtr<long long>("EventID");
            auto fWire_Channel_w = wireEntry->GetPtr<std::vector<unsigned int>>("fWire_Channel");
            auto fWire_View_w = wireEntry->GetPtr<std::vector<int>>("fWire_View");
            auto fSignalROI_nROIs_w = wireEntry->GetPtr<std::vector<unsigned int>>("fSignalROI_nROIs");
            auto fSignalROI_offsets_w = wireEntry->GetPtr<std::vector<std::size_t>>("fSignalROI_offsets");
            auto fSignalROI_data_w = wireEntry->GetPtr<std::vector<float>>("fSignalROI_data");
            
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                // Generate data (not timed)
                HitVector hit = std::move(generateRandomHitVector(eventIndex, hitsPerEvent, rng));
                WireVector wire = std::move(generateRandomWireVector(eventIndex, wiresPerEvent, wiresPerEvent, rng));
                
                // TIMING: RNTuple Storage Operations only
                storageTimer.Start();
                *eventID = hit.EventID;
                *fChannel = std::move(hit.getChannel());
                *fView = std::move(hit.getView());
                *fStartTick = std::move(hit.getStartTick());
                *fEndTick = std::move(hit.getEndTick());
                *fPeakTime = std::move(hit.getPeakTime());
                *fSigmaPeakTime = std::move(hit.getSigmaPeakTime());
                *fRMS = std::move(hit.getRMS());
                *fPeakAmplitude = std::move(hit.getPeakAmplitude());
                *fSigmaPeakAmplitude = std::move(hit.getSigmaPeakAmplitude());
                *fROISummedADC = std::move(hit.getROISummedADC());
                *fHitSummedADC = std::move(hit.getHitSummedADC());
                *fIntegral = std::move(hit.getIntegral());
                *fSigmaIntegral = std::move(hit.getSigmaIntegral());
                *fMultiplicity = std::move(hit.getMultiplicity());
                *fLocalIndex = std::move(hit.getLocalIndex());
                *fGoodnessOfFit = std::move(hit.getGoodnessOfFit());
                *fNDF = std::move(hit.getNDF());
                *fSignalType = std::move(hit.getSignalType());
                *fWireID_Cryostat = std::move(hit.getWireID_Cryostat());
                *fWireID_TPC = std::move(hit.getWireID_TPC());
                *fWireID_Plane = std::move(hit.getWireID_Plane());
                *fWireID_Wire = std::move(hit.getWireID_Wire());
                hitFillContext->Fill(*hitEntry);
                *eventID_w = wire.EventID;
                *fWire_Channel_w = wire.getWire_Channel();
                *fWire_View_w = wire.getWire_View();
                *fSignalROI_nROIs_w = wire.getSignalROI_nROIs();
                *fSignalROI_offsets_w = wire.getSignalROI_offsets();
                *fSignalROI_data_w = wire.getSignalROI_data();
                wireFillContext->Fill(*wireEntry);
                storageTimer.Stop();
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
        
        std::cout << "generateAndWriteSplitHitAndWireDataVector:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

void generateAndWriteSpilHitAndWireDataVector(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent = hitsPerEvent / numSpils;
    int adjustedWiresPerEvent = wiresPerEvent / numSpils; // keep total wires constant across spils
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
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
        // --- WIRE NTUPLE MODEL for Vector-based writers ---
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<int>("SpilID");
        wireModel->MakeField<std::vector<unsigned int>>("fWire_Channel");
        wireModel->MakeField<std::vector<int>>("fWire_View");
        wireModel->MakeField<std::vector<unsigned int>>("fSignalROI_nROIs");
        wireModel->MakeField<std::vector<std::size_t>>("fSignalROI_offsets");
        wireModel->MakeField<std::vector<float>>("fSignalROI_data");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        int total = numEvents * numSpils;
        
        int nThreads = 1;
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
            
            // Per-thread, per-entry field pointers
            auto eventID = hitEntry->GetPtr<long long>("EventID");
            auto spilID = hitEntry->GetPtr<int>("SpilID");
            auto fChannel = hitEntry->GetPtr<std::vector<unsigned int>>("Channel");
            auto fView = hitEntry->GetPtr<std::vector<int>>("View");
            auto fStartTick = hitEntry->GetPtr<std::vector<int>>("StartTick");
            auto fEndTick = hitEntry->GetPtr<std::vector<int>>("EndTick");
            auto fPeakTime = hitEntry->GetPtr<std::vector<float>>("PeakTime");
            auto fSigmaPeakTime = hitEntry->GetPtr<std::vector<float>>("SigmaPeakTime");
            auto fRMS = hitEntry->GetPtr<std::vector<float>>("RMS");
            auto fPeakAmplitude = hitEntry->GetPtr<std::vector<float>>("PeakAmplitude");
            auto fSigmaPeakAmplitude = hitEntry->GetPtr<std::vector<float>>("SigmaPeakAmplitude");
            auto fROISummedADC = hitEntry->GetPtr<std::vector<float>>("ROISummedADC");
            auto fHitSummedADC = hitEntry->GetPtr<std::vector<float>>("HitSummedADC");
            auto fIntegral = hitEntry->GetPtr<std::vector<float>>("Integral");
            auto fSigmaIntegral = hitEntry->GetPtr<std::vector<float>>("SigmaIntegral");
            auto fMultiplicity = hitEntry->GetPtr<std::vector<short int>>("Multiplicity");
            auto fLocalIndex = hitEntry->GetPtr<std::vector<short int>>("LocalIndex");
            auto fGoodnessOfFit = hitEntry->GetPtr<std::vector<float>>("GoodnessOfFit");
            auto fNDF = hitEntry->GetPtr<std::vector<int>>("NDF");
            auto fSignalType = hitEntry->GetPtr<std::vector<int>>("SignalType");
            auto fWireID_Cryostat = hitEntry->GetPtr<std::vector<int>>("WireID_Cryostat");
            auto fWireID_TPC = hitEntry->GetPtr<std::vector<int>>("WireID_TPC");
            auto fWireID_Plane = hitEntry->GetPtr<std::vector<int>>("WireID_Plane");
            auto fWireID_Wire = hitEntry->GetPtr<std::vector<int>>("WireID_Wire");
            auto eventID_w = wireEntry->GetPtr<long long>("EventID");
            auto spilID_w = wireEntry->GetPtr<int>("SpilID");
            auto fWire_Channel_w = wireEntry->GetPtr<std::vector<unsigned int>>("fWire_Channel");
            auto fWire_View_w = wireEntry->GetPtr<std::vector<int>>("fWire_View");
            auto fSignalROI_nROIs_w = wireEntry->GetPtr<std::vector<unsigned int>>("fSignalROI_nROIs");
            auto fSignalROI_offsets_w = wireEntry->GetPtr<std::vector<std::size_t>>("fSignalROI_offsets");
            auto fSignalROI_data_w = wireEntry->GetPtr<std::vector<float>>("fSignalROI_data");
            
            for (int idx = start; idx < end; ++idx) {
                int eventID_val = idx / numSpils;
                int spilID_val = idx % numSpils;
                long long uniqueEventID = static_cast<long long>(eventID_val) * 10000 + spilID_val;
                
                // Generate data (not timed)
                HitVector hit = std::move(generateRandomHitVector(uniqueEventID, adjustedHitsPerEvent, rng));
                WireVector wire = std::move(generateRandomWireVector(uniqueEventID, adjustedWiresPerEvent, wiresPerEvent, rng));
                
                // TIMING: RNTuple Storage Operations only
                storageTimer.Start();
                *eventID = hit.EventID;
                *spilID = spilID_val;
                *fChannel = std::move(hit.getChannel());
                *fView = std::move(hit.getView());
                *fStartTick = std::move(hit.getStartTick());
                *fEndTick = std::move(hit.getEndTick());
                *fPeakTime = std::move(hit.getPeakTime());
                *fSigmaPeakTime = std::move(hit.getSigmaPeakTime());
                *fRMS = std::move(hit.getRMS());
                *fPeakAmplitude = std::move(hit.getPeakAmplitude());
                *fSigmaPeakAmplitude = std::move(hit.getSigmaPeakAmplitude());
                *fROISummedADC = std::move(hit.getROISummedADC());
                *fHitSummedADC = std::move(hit.getHitSummedADC());
                *fIntegral = std::move(hit.getIntegral());
                *fSigmaIntegral = std::move(hit.getSigmaIntegral());
                *fMultiplicity = std::move(hit.getMultiplicity());
                *fLocalIndex = std::move(hit.getLocalIndex());
                *fGoodnessOfFit = std::move(hit.getGoodnessOfFit());
                *fNDF = std::move(hit.getNDF());
                *fSignalType = std::move(hit.getSignalType());
                *fWireID_Cryostat = std::move(hit.getWireID_Cryostat());
                *fWireID_TPC = std::move(hit.getWireID_TPC());
                *fWireID_Plane = std::move(hit.getWireID_Plane());
                *fWireID_Wire = std::move(hit.getWireID_Wire());
                hitFillContext->Fill(*hitEntry);
                *eventID_w = wire.EventID;
                *spilID_w = spilID_val;
                *fWire_Channel_w = wire.getWire_Channel();
                *fWire_View_w = wire.getWire_View();
                *fSignalROI_nROIs_w = wire.getSignalROI_nROIs();
                *fSignalROI_offsets_w = wire.getSignalROI_offsets();
                *fSignalROI_data_w = wire.getSignalROI_data();
                wireFillContext->Fill(*wireEntry);
                storageTimer.Stop();
            }
            
            // Return only storage timing
            return storageTimer.RealTime();
        };
        
        int chunk = total / nThreads;
        std::vector<std::future<double>> futures;
        
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? total : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        
        // Collect storage timing results from all threads
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        
        std::cout << "generateAndWriteSpilHitAndWireDataVector:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

void generateAndWriteHitWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
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
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<unsigned int>("fWire_Channel");
        wireModel->MakeField<int>("fWire_View");
        wireModel->MakeField<std::vector<RegionOfInterest>>("fSignalROI");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        
        // Use only one thread for this experiment
        int nThreads = 1;
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

            auto eventID_w = wireEntry->GetPtr<long long>("EventID");
            auto fWire_Channel_w = wireEntry->GetPtr<unsigned int>("fWire_Channel");
            auto fWire_View_w = wireEntry->GetPtr<int>("fWire_View");
            auto fSignalROI_w = wireEntry->GetPtr<std::vector<RegionOfInterest>>("fSignalROI");

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

                    *fWire_Channel_w = wire.fWire_Channel;
                    *fWire_View_w = wire.fWire_View;
                    *fSignalROI_w = wire.fSignalROI;
                    
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

void generateAndWriteSplitHitAndWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
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
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<unsigned int>("fWire_Channel");
        wireModel->MakeField<int>("fWire_View");
        wireModel->MakeField<std::vector<RegionOfInterest>>("fSignalROI");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        
        // Use only one thread for this experiment
        int nThreads = 1;
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

            auto eventID_w = wireEntry->GetPtr<long long>("EventID");
            auto fWire_Channel_w = wireEntry->GetPtr<unsigned int>("fWire_Channel");
            auto fWire_View_w = wireEntry->GetPtr<int>("fWire_View");
            auto fSignalROI_w = wireEntry->GetPtr<std::vector<RegionOfInterest>>("fSignalROI");

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

                    *fWire_Channel_w = wire.fWire_Channel;
                    *fWire_View_w = wire.fWire_View;
                    *fSignalROI_w = wire.fSignalROI;
                    
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
        
        std::cout << "generateAndWriteSplitHitAndWireDataIndividual:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

void generateAndWriteSpilHitAndWireDataIndividual(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent = hitsPerEvent / numSpils;
    int adjustedWiresPerEvent = wiresPerEvent / numSpils; // keep total wires constant across spils
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE MODEL (Individual) ---
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

        // --- WIRE NTUPLE MODEL (Individual) ---
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<unsigned int>("fWire_Channel");
        wireModel->MakeField<int>("fWire_View");
        wireModel->MakeField<std::vector<RegionOfInterest>>("fSignalROI");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        // Use only one thread for this experiment
        int nThreads = 1;
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

            auto eventID_f = hitEntry->GetPtr<long long>("EventID");
            auto spilID_f = hitEntry->GetPtr<int>("SpilID");
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

            auto eventID_w = wireEntry->GetPtr<long long>("EventID");
            auto fWire_Channel_w = wireEntry->GetPtr<unsigned int>("fWire_Channel");
            auto fWire_View_w = wireEntry->GetPtr<int>("fWire_View");
            auto fSignalROI_w = wireEntry->GetPtr<std::vector<RegionOfInterest>>("fSignalROI");

            for (int idx = start; idx < end; ++idx) {
                int eventID_val = idx / numSpils;
                int spilID_val = idx % numSpils;
                long long uniqueEventID = static_cast<long long>(eventID_val) * 10000 + spilID_val;
                
                // Generate multiple hits and wires for this event to match Vector data volume
                for (int hitIndex = 0; hitIndex < adjustedHitsPerEvent; ++hitIndex) {
                    // Generate data (not timed)
                    HitIndividual hit = generateRandomHitIndividual(uniqueEventID, rng);
                    WireIndividual wire = generateRandomWireIndividual(uniqueEventID, wiresPerEvent, rng);
                    
                    // TIMING: RNTuple Storage Operations only
                    storageTimer.Start();
                    *eventID_f = hit.EventID;
                    *spilID_f = spilID_val;
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

                    *fWire_Channel_w = wire.fWire_Channel;
                    *fWire_View_w = wire.fWire_View;
                    *fSignalROI_w = wire.fSignalROI;
                    
                    hitFillContext->Fill(*hitEntry);
                    wireFillContext->Fill(*wireEntry);
                    storageTimer.Stop();
                }
            }
            
            // Return only storage timing
            return storageTimer.RealTime();
        };
        
        int chunk = numEvents * numSpils / nThreads;
        std::vector<std::future<double>> futures;
        
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? numEvents * numSpils : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        
        // Collect storage timing results from all threads
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        
        std::cout << "generateAndWriteSpilHitAndWireDataIndividual:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

// --- New: Store entire HitVector and WireVector as single fields (Vector-based) ---
void generateAndWriteHitWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitVector>("HitVector");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            auto hitObj = hitEntry->GetPtr<HitVector>("HitVector");
            auto wireObj = wireEntry->GetPtr<WireVector>("WireVector");
            TStopwatch storageTimer;
            storageTimer.Reset();
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                HitVector hit = generateRandomHitVector(eventIndex, hitsPerEvent, rng);
                WireVector wire = generateRandomWireVector(eventIndex, wiresPerEvent, rng);
                storageTimer.Start();
                *hitObj = std::move(hit);
                *wireObj = std::move(wire);
                hitFillContext->Fill(*hitEntry);
                wireFillContext->Fill(*wireEntry);
                storageTimer.Stop();
            }
            return storageTimer.RealTime();
        };
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futures;
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        std::cout << "generateAndWriteHitWireDataVectorDict:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

// --- New: Store entire HitIndividual and WireIndividual as single fields (Individual-based) ---
void generateAndWriteHitWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitIndividual>("HitIndividual");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            auto hitObj = hitEntry->GetPtr<HitIndividual>("HitIndividual");
            auto wireObj = wireEntry->GetPtr<WireIndividual>("WireIndividual");
            TStopwatch storageTimer;
            storageTimer.Reset();
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
                    HitIndividual hit = generateRandomHitIndividual(eventIndex, rng);
                    WireIndividual wire = generateRandomWireIndividual(eventIndex, wiresPerEvent, rng);
                    storageTimer.Start();
                    *hitObj = std::move(hit);
                    *wireObj = std::move(wire);
                    hitFillContext->Fill(*hitEntry);
                    wireFillContext->Fill(*wireEntry);
                    storageTimer.Stop();
                }
            }
            return storageTimer.RealTime();
        };
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futures;
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        std::cout << "generateAndWriteHitWireDataIndividualDict:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

// --- New: Split Vector-based, store as objects ---
void generateAndWriteSplitHitAndWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitVector>("HitVector");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            auto hitObj = hitEntry->GetPtr<HitVector>("HitVector");
            auto wireObj = wireEntry->GetPtr<WireVector>("WireVector");
            TStopwatch storageTimer;
            storageTimer.Reset();
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                HitVector hit = generateRandomHitVector(eventIndex, hitsPerEvent, rng);
                WireVector wire = generateRandomWireVector(eventIndex, wiresPerEvent, rng);
                storageTimer.Start();
                *hitObj = std::move(hit);
                *wireObj = std::move(wire);
                hitFillContext->Fill(*hitEntry);
                wireFillContext->Fill(*wireEntry);
                storageTimer.Stop();
            }
            return storageTimer.RealTime();
        };
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futures;
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        std::cout << "generateAndWriteSplitHitAndWireDataVectorDict:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

// --- New: Split Individual-based, store as objects ---
void generateAndWriteSplitHitAndWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitIndividual>("HitIndividual");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            auto hitObj = hitEntry->GetPtr<HitIndividual>("HitIndividual");
            auto wireObj = wireEntry->GetPtr<WireIndividual>("WireIndividual");
            TStopwatch storageTimer;
            storageTimer.Reset();
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
                    HitIndividual hit = generateRandomHitIndividual(eventIndex, rng);
                    WireIndividual wire = generateRandomWireIndividual(eventIndex, wiresPerEvent, rng);
                    storageTimer.Start();
                    *hitObj = std::move(hit);
                    *wireObj = std::move(wire);
                    hitFillContext->Fill(*hitEntry);
                    wireFillContext->Fill(*wireEntry);
                    storageTimer.Stop();
                }
            }
            return storageTimer.RealTime();
        };
        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futures;
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        std::cout << "generateAndWriteSplitHitAndWireDataIndividualDict:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

// --- New: Spil Vector-based, store as objects ---
void generateAndWriteSpilHitAndWireDataVectorDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent = hitsPerEvent / numSpils;
    int adjustedWiresPerEvent = wiresPerEvent / numSpils; // keep total wires constant across spils
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitVector>("HitVector");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireVector>("WireVector");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        int total = numEvents * numSpils;
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            auto hitObj = hitEntry->GetPtr<HitVector>("HitVector");
            auto wireObj = wireEntry->GetPtr<WireVector>("WireVector");
            TStopwatch storageTimer;
            storageTimer.Reset();
            for (int idx = start; idx < end; ++idx) {
                int eventID_val = idx / numSpils;
                int spilID_val = idx % numSpils;
                long long uniqueEventID = static_cast<long long>(eventID_val) * 10000 + spilID_val;
                HitVector hit = generateRandomHitVector(uniqueEventID, adjustedHitsPerEvent, rng);
                WireVector wire = generateRandomWireVector(uniqueEventID, adjustedWiresPerEvent, wiresPerEvent, rng);
                storageTimer.Start();
                *hitObj = std::move(hit);
                *wireObj = std::move(wire);
                hitFillContext->Fill(*hitEntry);
                wireFillContext->Fill(*wireEntry);
                storageTimer.Stop();
            }
            return storageTimer.RealTime();
        };
        int chunk = total / nThreads;
        std::vector<std::future<double>> futures;
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? total : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        std::cout << "generateAndWriteSpilHitAndWireDataVectorDict:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

// --- New: Spil Individual-based, store as objects ---
void generateAndWriteSpilHitAndWireDataIndividualDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent = hitsPerEvent / numSpils;
    int adjustedWiresPerEvent = wiresPerEvent / numSpils; // keep total wires constant across spils
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE MODEL (Individual) ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<HitIndividual>("HitIndividual");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<WireIndividual>("WireIndividual");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);
        int total = numEvents * numSpils;
        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);
        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            auto hitObj = hitEntry->GetPtr<HitIndividual>("HitIndividual");
            auto wireObj = wireEntry->GetPtr<WireIndividual>("WireIndividual");
            TStopwatch storageTimer;
            storageTimer.Reset();
            for (int idx = start; idx < end; ++idx) {
                int eventID_val = idx / numSpils;
                int spilID_val = idx % numSpils;
                long long uniqueEventID = static_cast<long long>(eventID_val) * 10000 + spilID_val;
                for (int hitIndex = 0; hitIndex < adjustedHitsPerEvent; ++hitIndex) {
                    HitIndividual hit = generateRandomHitIndividual(uniqueEventID, rng);
                    WireIndividual wire = generateRandomWireIndividual(uniqueEventID, adjustedWiresPerEvent, wiresPerEvent, rng);
                    storageTimer.Start();
                    *hitObj = std::move(hit);
                    *wireObj = std::move(wire);
                    hitFillContext->Fill(*hitEntry);
                    wireFillContext->Fill(*wireEntry);
                    storageTimer.Stop();
                }
            }
            return storageTimer.RealTime();
        };
        int chunk = total / nThreads;
        std::vector<std::future<double>> futures;
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? total : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        std::cout << "generateAndWriteSpilHitAndWireDataIndividualDict:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
} 

void generateAndWriteHitWireDataVectorOfIndividuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./output");
    TFile file(fileName.c_str(), "RECREATE");
    {
        // --- HIT NTUPLE ---
        auto hitModel = ROOT::RNTupleModel::Create();
        hitModel->MakeField<long long>("EventID");
        hitModel->MakeField<std::vector<HitIndividual>>("Hits");
        auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "hits", file);
        // --- WIRE NTUPLE ---
        auto wireModel = ROOT::RNTupleModel::Create();
        wireModel->MakeField<long long>("EventID");
        wireModel->MakeField<std::vector<WireIndividual>>("Wires");
        auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "wires", file);

        int nThreads = 1;
        auto seeds = Utils::generateSeeds(nThreads);

        auto fillFunc = [&](int start, int end, unsigned int seed) {
            std::mt19937 rng(seed);
            auto hitFillContext = hitWriter->CreateFillContext();
            auto hitEntry = hitFillContext->CreateEntry();
            auto wireFillContext = wireWriter->CreateFillContext();
            auto wireEntry = wireFillContext->CreateEntry();
            auto eventID = hitEntry->GetPtr<long long>("EventID");
            auto hitsVec = hitEntry->GetPtr<std::vector<HitIndividual>>("Hits");
            auto eventID_w = wireEntry->GetPtr<long long>("EventID");
            auto wiresVec = wireEntry->GetPtr<std::vector<WireIndividual>>("Wires");
            TStopwatch storageTimer;
            storageTimer.Reset();
            for (int eventIndex = start; eventIndex < end; ++eventIndex) {
                std::vector<HitIndividual> hits;
                hits.reserve(hitsPerEvent);
                for (int i = 0; i < hitsPerEvent; ++i) {
                    hits.push_back(generateRandomHitIndividual(eventIndex, rng));
                }
                std::vector<WireIndividual> wires;
                wires.reserve(wiresPerEvent);
                for (int i = 0; i < wiresPerEvent; ++i) {
                    wires.push_back(generateRandomWireIndividual(eventIndex, wiresPerEvent, rng));
                }
                storageTimer.Start();
                *eventID = eventIndex;
                *hitsVec = std::move(hits);
                hitFillContext->Fill(*hitEntry);
                *eventID_w = eventIndex;
                *wiresVec = std::move(wires);
                wireFillContext->Fill(*wireEntry);
                storageTimer.Stop();
            }
            return storageTimer.RealTime();
        };

        int chunk = numEvents / nThreads;
        std::vector<std::future<double>> futures;
        for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
            int start = threadIndex * chunk;
            int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
            futures.push_back(std::async(std::launch::async, fillFunc, start, end, seeds[threadIndex]));
        }
        double totalStorageTime = 0.0;
        for (auto& future : futures) {
            totalStorageTime += future.get();
        }
        std::cout << "generateAndWriteHitWireDataVectorOfIndividuals:" << std::endl;
        std::cout << "  RNTuple Storage Time: " << totalStorageTime * 1000 << " ms" << std::endl;
    }
}

void out() {
    int numEvents = 1000;
    int hitsPerEvent = 100;
    int wiresPerEvent = 100;
    int numSpils = 10;

    std::cout << "Generating HitWire data with Vector format..." << std::endl;
    generateAndWriteHitWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, "./output/vector.root");
    std::cout << "Generating HitWire data with Individual format..." << std::endl;
    generateAndWriteHitWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, "./output/individual.root");
    std::cout << "Generating Split HitWire data with Vector format..." << std::endl;
    generateAndWriteSplitHitAndWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_vector.root");
    std::cout << "Generating Split HitWire data with Individual format..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_individual.root");
    std::cout << "Generating Spil HitWire data with Vector format..." << std::endl;
    generateAndWriteSpilHitAndWireDataVector(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_vector.root");
    std::cout << "Generating Spil HitWire data with Individual format..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividual(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_individual.root");
    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Generating HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteHitWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/vector_dict.root");
    std::cout << "Generating HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteHitWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/individual_dict.root");
    std::cout << "Generating Split HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_vector_dict.root");
    std::cout << "Generating Split HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSplitHitAndWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, "./output/split_individual_dict.root");
    std::cout << "Generating Spil HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataVectorDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_vector_dict.root");
    std::cout << "Generating Spil HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWriteSpilHitAndWireDataIndividualDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, "./output/spil_individual_dict.root");
    std::cout << "Generating HitWire data with Vector of Individuals format..." << std::endl;
    generateAndWriteHitWireDataVectorOfIndividuals(numEvents, hitsPerEvent, wiresPerEvent, "./output/vector_of_individuals.root");
}
