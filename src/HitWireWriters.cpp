#include "HitWireWriters.hpp"
#include "Hit.hpp"
#include "Wire.hpp"
#include "HitWireGenerators.hpp"
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>
#include <ROOT/RNTupleParallelWriter.hxx>
#include <TFile.h>
#include <filesystem>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>
#include <random>
#include <utility>

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
    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    wireModel->MakeField<std::vector<int>>("Wire_View");
    wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireWireVector", file);
    // --- Parallel Fill Function ---
    auto fillFunc = [&](int start, int end) {
        std::mt19937 rng(std::random_device{}());
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
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
        auto fWire_Channel = wireEntry->GetPtr<std::vector<unsigned int>>("Wire_Channel");
        auto fWire_View = wireEntry->GetPtr<std::vector<int>>("Wire_View");
        auto fSignalROI_nROIs = wireEntry->GetPtr<std::vector<unsigned int>>("SignalROI_nROIs");
        auto fSignalROI_offsets = wireEntry->GetPtr<std::vector<std::size_t>>("SignalROI_offsets");
        auto fSignalROI_data = wireEntry->GetPtr<std::vector<float>>("SignalROI_data");
        for (int eventIndex = start; eventIndex < end; ++eventIndex) {
            HitVector hit = generateRandomHitVector(eventIndex, hitsPerEvent, rng);
            WireVector wire = generateRandomWireVector(eventIndex, hitsPerEvent, rng);
            *eventID = hit.EventID;
            *fChannel = hit.getChannel();
            *fView = hit.getView();
            *fStartTick = hit.getStartTick();
            *fEndTick = hit.getEndTick();
            *fPeakTime = hit.getPeakTime();
            *fSigmaPeakTime = hit.getSigmaPeakTime();
            *fRMS = hit.getRMS();
            *fPeakAmplitude = hit.getPeakAmplitude();
            *fSigmaPeakAmplitude = hit.getSigmaPeakAmplitude();
            *fROISummedADC = hit.getROISummedADC();
            *fHitSummedADC = hit.getHitSummedADC();
            *fIntegral = hit.getIntegral();
            *fSigmaIntegral = hit.getSigmaIntegral();
            *fMultiplicity = hit.getMultiplicity();
            *fLocalIndex = hit.getLocalIndex();
            *fGoodnessOfFit = hit.getGoodnessOfFit();
            *fNDF = hit.getNDF();
            *fSignalType = hit.getSignalType();
            *fWireID_Cryostat = hit.getWireID_Cryostat();
            *fWireID_TPC = hit.getWireID_TPC();
            *fWireID_Plane = hit.getWireID_Plane();
            *fWireID_Wire = hit.getWireID_Wire();
            hitFillContext->Fill(*hitEntry);
            *eventID_w = wire.EventID;
            *fWire_Channel = wire.getWire_Channel();
            *fWire_View = wire.getWire_View();
            *fSignalROI_nROIs = wire.getSignalROI_nROIs();
            *fSignalROI_offsets = wire.getSignalROI_offsets();
            *fSignalROI_data = wire.getSignalROI_data();
            wireFillContext->Fill(*wireEntry);
        }
    };
    int nThreads = std::thread::hardware_concurrency();
    int chunk = numEvents / nThreads;
    std::vector<std::thread> threads;
    auto chrono_start = std::chrono::high_resolution_clock::now();
    for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
        int start = threadIndex * chunk;
        int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    auto chrono_end = std::chrono::high_resolution_clock::now();
    std::cout << "generateAndWriteHitWireDataVector took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(chrono_end - chrono_start).count()
              << " ms\n";
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
    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    wireModel->MakeField<std::vector<int>>("Wire_View");
    wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireVector", file);
    // --- Parallel Fill Function ---
    auto fillFunc = [&](int start, int end) {
        std::mt19937 rng(std::random_device{}());
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
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
        auto fWire_Channel = wireEntry->GetPtr<std::vector<unsigned int>>("Wire_Channel");
        auto fWire_View = wireEntry->GetPtr<std::vector<int>>("Wire_View");
        auto fSignalROI_nROIs = wireEntry->GetPtr<std::vector<unsigned int>>("SignalROI_nROIs");
        auto fSignalROI_offsets = wireEntry->GetPtr<std::vector<std::size_t>>("SignalROI_offsets");
        auto fSignalROI_data = wireEntry->GetPtr<std::vector<float>>("SignalROI_data");
        for (int eventIndex = start; eventIndex < end; ++eventIndex) {
            HitVector hit = generateRandomHitVector(eventIndex, hitsPerEvent, rng);
            WireVector wire = generateRandomWireVector(eventIndex, hitsPerEvent, rng);
            *eventID = hit.EventID;
            *fChannel = hit.getChannel();
            *fView = hit.getView();
            *fStartTick = hit.getStartTick();
            *fEndTick = hit.getEndTick();
            *fPeakTime = hit.getPeakTime();
            *fSigmaPeakTime = hit.getSigmaPeakTime();
            *fRMS = hit.getRMS();
            *fPeakAmplitude = hit.getPeakAmplitude();
            *fSigmaPeakAmplitude = hit.getSigmaPeakAmplitude();
            *fROISummedADC = hit.getROISummedADC();
            *fHitSummedADC = hit.getHitSummedADC();
            *fIntegral = hit.getIntegral();
            *fSigmaIntegral = hit.getSigmaIntegral();
            *fMultiplicity = hit.getMultiplicity();
            *fLocalIndex = hit.getLocalIndex();
            *fGoodnessOfFit = hit.getGoodnessOfFit();
            *fNDF = hit.getNDF();
            *fSignalType = hit.getSignalType();
            *fWireID_Cryostat = hit.getWireID_Cryostat();
            *fWireID_TPC = hit.getWireID_TPC();
            *fWireID_Plane = hit.getWireID_Plane();
            *fWireID_Wire = hit.getWireID_Wire();
            hitFillContext->Fill(*hitEntry);
            *eventID_w = wire.EventID;
            *fWire_Channel = wire.getWire_Channel();
            *fWire_View = wire.getWire_View();
            *fSignalROI_nROIs = wire.getSignalROI_nROIs();
            *fSignalROI_offsets = wire.getSignalROI_offsets();
            *fSignalROI_data = wire.getSignalROI_data();
            wireFillContext->Fill(*wireEntry);
        }
    };
    int nThreads = std::thread::hardware_concurrency();
    int chunk = numEvents / nThreads;
    std::vector<std::thread> threads;
    auto chrono_start = std::chrono::high_resolution_clock::now();
    for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
        int start = threadIndex * chunk;
        int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    auto chrono_end = std::chrono::high_resolution_clock::now();
    std::cout << "generateAndWriteSplitHitAndWireDataVector took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(chrono_end - chrono_start).count()
              << " ms\n";
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
    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<int>("SpilID");
    wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    wireModel->MakeField<std::vector<int>>("Wire_View");
    wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireSpilVector", file);
    int total = numEvents * numSpils;
    auto fillFunc = [&](int start, int end) {
        std::mt19937 rng(std::random_device{}());
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
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
        auto fWire_Channel = wireEntry->GetPtr<std::vector<unsigned int>>("Wire_Channel");
        auto fWire_View = wireEntry->GetPtr<std::vector<int>>("Wire_View");
        auto fSignalROI_nROIs = wireEntry->GetPtr<std::vector<unsigned int>>("SignalROI_nROIs");
        auto fSignalROI_offsets = wireEntry->GetPtr<std::vector<std::size_t>>("SignalROI_offsets");
        auto fSignalROI_data = wireEntry->GetPtr<std::vector<float>>("SignalROI_data");
        for (int idx = start; idx < end; ++idx) {
            int eventID_val = idx / numSpils;
            int spilID_val = idx % numSpils;
            long long uniqueEventID = static_cast<long long>(eventID_val) * 10000 + spilID_val;
            HitVector hit = generateRandomHitVector(uniqueEventID, adjustedHitsPerEvent, rng);
            WireVector wire = generateRandomWireVector(uniqueEventID, adjustedHitsPerEvent, rng);
            *eventID = hit.EventID;
            *spilID = spilID_val;
            *fChannel = hit.getChannel();
            *fView = hit.getView();
            *fStartTick = hit.getStartTick();
            *fEndTick = hit.getEndTick();
            *fPeakTime = hit.getPeakTime();
            *fSigmaPeakTime = hit.getSigmaPeakTime();
            *fRMS = hit.getRMS();
            *fPeakAmplitude = hit.getPeakAmplitude();
            *fSigmaPeakAmplitude = hit.getSigmaPeakAmplitude();
            *fROISummedADC = hit.getROISummedADC();
            *fHitSummedADC = hit.getHitSummedADC();
            *fIntegral = hit.getIntegral();
            *fSigmaIntegral = hit.getSigmaIntegral();
            *fMultiplicity = hit.getMultiplicity();
            *fLocalIndex = hit.getLocalIndex();
            *fGoodnessOfFit = hit.getGoodnessOfFit();
            *fNDF = hit.getNDF();
            *fSignalType = hit.getSignalType();
            *fWireID_Cryostat = hit.getWireID_Cryostat();
            *fWireID_TPC = hit.getWireID_TPC();
            *fWireID_Plane = hit.getWireID_Plane();
            *fWireID_Wire = hit.getWireID_Wire();
            hitFillContext->Fill(*hitEntry);
            *eventID_w = wire.EventID;
            *spilID_w = spilID_val;
            *fWire_Channel = wire.getWire_Channel();
            *fWire_View = wire.getWire_View();
            *fSignalROI_nROIs = wire.getSignalROI_nROIs();
            *fSignalROI_offsets = wire.getSignalROI_offsets();
            *fSignalROI_data = wire.getSignalROI_data();
            wireFillContext->Fill(*wireEntry);
        }
    };
    int nThreads = std::thread::hardware_concurrency();
    int chunk = total / nThreads;
    std::vector<std::thread> threads;
    auto chrono_start = std::chrono::high_resolution_clock::now();
    for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
        int start = threadIndex * chunk;
        int end = (threadIndex == nThreads - 1) ? total : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    auto chrono_end = std::chrono::high_resolution_clock::now();
    std::cout << "generateAndWriteSpilHitAndWireDataVector took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(chrono_end - chrono_start).count()
              << " ms\n";
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

    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<unsigned int>("Wire_Channel");
    wireModel->MakeField<int>("Wire_View");
    wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireIndividual", file);
    
    auto fillFunc = [&](int start, int end) {
        std::mt19937 rng(std::random_device{}());
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
        
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
        auto fWire_Channel_w = wireEntry->GetPtr<unsigned int>("Wire_Channel");
        auto fWire_View_w = wireEntry->GetPtr<int>("Wire_View");
        auto fSignalROI_offsets = wireEntry->GetPtr<std::vector<std::size_t>>("SignalROI_offsets");
        auto fSignalROI_data = wireEntry->GetPtr<std::vector<float>>("SignalROI_data");

        for (int eventIndex = start; eventIndex < end; ++eventIndex) {
            // Generate multiple hits and wires for this event to match Vector data volume
            for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
                HitIndividual hit = generateRandomHitIndividual(eventIndex, rng);
                WireIndividual wire = generateRandomWireIndividual(eventIndex, hitsPerEvent, rng);
                
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

                *eventID_w = wire.EventID;
                *fWire_Channel_w = wire.fWire_Channel;
                *fWire_View_w = wire.fWire_View;
                *fSignalROI_offsets = wire.fSignalROI_offsets;
                *fSignalROI_data = wire.fSignalROI_data;
                
                hitFillContext->Fill(*hitEntry);
                wireFillContext->Fill(*wireEntry);
            }
        }
    };

    int nThreads = std::thread::hardware_concurrency();
    int chunk = numEvents / nThreads;
    std::vector<std::thread> threads;
    auto chrono_start = std::chrono::high_resolution_clock::now();
    for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
        int start = threadIndex * chunk;
        int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    auto chrono_end = std::chrono::high_resolution_clock::now();
    std::cout << "generateAndWriteHitWireDataIndividual took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(chrono_end - chrono_start).count()
              << " ms\n";
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

    auto wireModel = ROOT::RNTupleModel::Create();
    wireModel->MakeField<long long>("EventID");
    wireModel->MakeField<unsigned int>("Wire_Channel");
    wireModel->MakeField<int>("Wire_View");
    wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireIndividual", file);
    
    auto fillFunc = [&](int start, int end) {
        std::mt19937 rng(std::random_device{}());
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
        
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
        auto fWire_Channel_w = wireEntry->GetPtr<unsigned int>("Wire_Channel");
        auto fWire_View_w = wireEntry->GetPtr<int>("Wire_View");
        auto fSignalROI_offsets = wireEntry->GetPtr<std::vector<std::size_t>>("SignalROI_offsets");
        auto fSignalROI_data = wireEntry->GetPtr<std::vector<float>>("SignalROI_data");

        for (int eventIndex = start; eventIndex < end; ++eventIndex) {
            // Generate multiple hits and wires for this event to match Vector data volume
            for (int hitIndex = 0; hitIndex < hitsPerEvent; ++hitIndex) {
                HitIndividual hit = generateRandomHitIndividual(eventIndex, rng);
                WireIndividual wire = generateRandomWireIndividual(eventIndex, hitsPerEvent, rng);
                
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

                *eventID_w = wire.EventID;
                *fWire_Channel_w = wire.fWire_Channel;
                *fWire_View_w = wire.fWire_View;
                *fSignalROI_offsets = wire.fSignalROI_offsets;
                *fSignalROI_data = wire.fSignalROI_data;
                
                hitFillContext->Fill(*hitEntry);
                wireFillContext->Fill(*wireEntry);
            }
        }
    };

    int nThreads = std::thread::hardware_concurrency();
    int chunk = numEvents / nThreads;
    std::vector<std::thread> threads;
    auto chrono_start = std::chrono::high_resolution_clock::now();
    for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
        int start = threadIndex * chunk;
        int end = (threadIndex == nThreads - 1) ? numEvents : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    auto chrono_end = std::chrono::high_resolution_clock::now();
    std::cout << "generateAndWriteSplitHitAndWireDataIndividual took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(chrono_end - chrono_start).count()
              << " ms\n";
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
    wireModel->MakeField<int>("SpilID");
    wireModel->MakeField<unsigned int>("Wire_Channel");
    wireModel->MakeField<int>("Wire_View");
    wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireSpilIndividual", file);

    std::vector<std::thread> threads;

    auto fillFunc = [&](int start, int end) {
        std::mt19937 rng(std::random_device{}());
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();

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
        auto spilID_w = wireEntry->GetPtr<int>("SpilID");
        auto fWire_Channel_w = wireEntry->GetPtr<unsigned int>("Wire_Channel");
        auto fWire_View_w = wireEntry->GetPtr<int>("Wire_View");
        auto fSignalROI_offsets = wireEntry->GetPtr<std::vector<std::size_t>>("SignalROI_offsets");
        auto fSignalROI_data = wireEntry->GetPtr<std::vector<float>>("SignalROI_data");

        for (int idx = start; idx < end; ++idx) {
            int eventID_val = idx / numSpils;
            int spilID_val = idx % numSpils;
            long long uniqueEventID = static_cast<long long>(eventID_val) * 10000 + spilID_val;
            
            // Generate multiple hits and wires for this event to match Vector data volume
            for (int hitIndex = 0; hitIndex < adjustedHitsPerEvent; ++hitIndex) {
                HitIndividual hit = generateRandomHitIndividual(uniqueEventID, rng);
                WireIndividual wire = generateRandomWireIndividual(uniqueEventID, adjustedHitsPerEvent, rng);

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

                *eventID_w = wire.EventID;
                *spilID_w = spilID_val;
                *fWire_Channel_w = wire.fWire_Channel;
                *fWire_View_w = wire.fWire_View;
                *fSignalROI_offsets = std::move(wire.getSignalROI_offsets());
                *fSignalROI_data = std::move(wire.getSignalROI_data());

                hitFillContext->Fill(*hitEntry);
                wireFillContext->Fill(*wireEntry);
            }
        }
    };
    
    int nThreads = std::thread::hardware_concurrency();
    int chunk = numEvents * numSpils / nThreads;
    threads.reserve(nThreads);
    auto chrono_start = std::chrono::high_resolution_clock::now();
    for (int threadIndex = 0; threadIndex < nThreads; ++threadIndex) {
        int start = threadIndex * chunk;
        int end = (threadIndex == nThreads - 1) ? numEvents * numSpils : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    auto chrono_end = std::chrono::high_resolution_clock::now();
    std::cout << "generateAndWriteSpilHitAndWireDataIndividual took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(chrono_end - chrono_start).count()
              << " ms\n";
} 
