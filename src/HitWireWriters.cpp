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

void generateAndWriteHitWireDataVector(int numEvents, int hitsPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./hitwire");
    TFile file(fileName.c_str(), "UPDATE");
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitWireVector", file);
    // --- WIRE NTUPLE MODEL for Vector-based writers ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<std::vector<unsigned int>>("fWire_Channel");
    wireModel->MakeField<std::vector<int>>("fWire_View");
    wireModel->MakeField<std::vector<unsigned int>>("fSignalROI_nROIs");
    wireModel->MakeField<std::vector<std::size_t>>("fSignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("fSignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireVector", file);
    
    int nThreads = std::thread::hardware_concurrency();
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
            WireVector wire = std::move(generateRandomWireVector(eventIndex, hitsPerEvent, rng));
            
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
    
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSplitHitAndWireDataVector(int numEvents, int hitsPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./hitwire");
    TFile file(fileName.c_str(), "UPDATE");
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitVector", file);
    // --- WIRE NTUPLE MODEL for Vector-based writers ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<std::vector<unsigned int>>("fWire_Channel");
    wireModel->MakeField<std::vector<int>>("fWire_View");
    wireModel->MakeField<std::vector<unsigned int>>("fSignalROI_nROIs");
    wireModel->MakeField<std::vector<std::size_t>>("fSignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("fSignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireVector", file);
    
    int nThreads = std::thread::hardware_concurrency();
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
            WireVector wire = std::move(generateRandomWireVector(eventIndex, hitsPerEvent, rng));
            
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
    
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSpilHitAndWireDataVector(int numEvents, int numSpils, int hitsPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent = hitsPerEvent / numSpils;
    std::filesystem::create_directories("./hitwire");
    std::filesystem::create_directories("./hitwire/hitspils");
    std::filesystem::create_directories("./hitwire/wirespils");
    TFile file(fileName.c_str(), "UPDATE");
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitSpilVector", file);
    // --- WIRE NTUPLE MODEL for Vector-based writers ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<int>("SpilID");
    wireModel->MakeField<std::vector<unsigned int>>("fWire_Channel");
    wireModel->MakeField<std::vector<int>>("fWire_View");
    wireModel->MakeField<std::vector<unsigned int>>("fSignalROI_nROIs");
    wireModel->MakeField<std::vector<std::size_t>>("fSignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("fSignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireSpilVector", file);
    int total = numEvents * numSpils;
    
    int nThreads = std::thread::hardware_concurrency();
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
            WireVector wire = std::move(generateRandomWireVector(uniqueEventID, adjustedHitsPerEvent, rng));
            
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
    
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteHitWireDataIndividual(int numEvents, int hitsPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./hitwire");
    TFile file(fileName.c_str(), "RECREATE");
    
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitWireIndividual", file);

    // --- WIRE NTUPLE MODEL for Individual-based writers ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<unsigned int>("fWire_Channel");
    wireModel->MakeField<int>("fWire_View");
    wireModel->MakeField<std::vector<RegionOfInterest>>("fSignalROI");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireIndividual", file);
    
    int nThreads = std::thread::hardware_concurrency();
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
                WireIndividual wire = generateRandomWireIndividual(eventIndex, hitsPerEvent, rng);
                
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
    
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSplitHitAndWireDataIndividual(int numEvents, int hitsPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./hitwire");
    TFile file(fileName.c_str(), "RECREATE");
    
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitIndividual", file);

    // --- WIRE NTUPLE MODEL for Individual-based writers ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<unsigned int>("fWire_Channel");
    wireModel->MakeField<int>("fWire_View");
    wireModel->MakeField<std::vector<RegionOfInterest>>("fSignalROI");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireIndividual", file);
    
    int nThreads = std::thread::hardware_concurrency();
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
                WireIndividual wire = generateRandomWireIndividual(eventIndex, hitsPerEvent, rng);
                
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
    
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSpilHitAndWireDataIndividual(int numEvents, int numSpils, int hitsPerEvent, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    int adjustedHitsPerEvent = hitsPerEvent / numSpils;
    std::filesystem::create_directories("./hitwire/hitspils");
    std::filesystem::create_directories("./hitwire/wirespils");
    TFile file(fileName.c_str(), "UPDATE");

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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitSpilIndividual", file);

    // --- WIRE NTUPLE MODEL (Individual) ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<unsigned int>("fWire_Channel");
    wireModel->MakeField<int>("fWire_View");
    wireModel->MakeField<std::vector<RegionOfInterest>>("fSignalROI");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireSpilIndividual", file);

    int nThreads = std::thread::hardware_concurrency();
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
                WireIndividual wire = generateRandomWireIndividual(uniqueEventID, adjustedHitsPerEvent, rng);
                
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
    
    // Writers go out of scope and flush/close automatically
} 
