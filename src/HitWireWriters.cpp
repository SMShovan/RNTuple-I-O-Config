#include "HitWireWriters.hpp"
#include "Hit.hpp"
#include "Wire.hpp"
#include "HitWireGenerators.hpp"
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>
#include <ROOT/RNTupleParallelWriter.hxx>
#include <TFile.h>
#include <iostream>
#include <filesystem>
#include <thread>
#include <vector>

void generateAndWriteHitWireDataSoA(int eventCount, int fieldSize, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./hitwire");
    TFile file(fileName.c_str(), "UPDATE");
    // --- HIT NTUPLE ---
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID = hitModel->MakeField<long long>("EventID");
    auto fChannel = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    auto fView = hitModel->MakeField<std::vector<int>>("View");
    auto fStartTick = hitModel->MakeField<std::vector<int>>("StartTick");
    auto fEndTick = hitModel->MakeField<std::vector<int>>("EndTick");
    auto fPeakTime = hitModel->MakeField<std::vector<float>>("PeakTime");
    auto fSigmaPeakTime = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    auto fRMS = hitModel->MakeField<std::vector<float>>("RMS");
    auto fPeakAmplitude = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    auto fSigmaPeakAmplitude = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    auto fROISummedADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    auto fHitSummedADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    auto fIntegral = hitModel->MakeField<std::vector<float>>("Integral");
    auto fSigmaIntegral = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    auto fMultiplicity = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    auto fLocalIndex = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    auto fGoodnessOfFit = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    auto fNDF = hitModel->MakeField<std::vector<int>>("NDF");
    auto fSignalType = hitModel->MakeField<std::vector<int>>("SignalType");
    auto fWireID_Cryostat = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    auto fWireID_TPC = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    auto fWireID_Plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    auto fWireID_Wire = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitWireSoA", file);
    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_w = wireModel->MakeField<long long>("EventID");
    auto fWire_Channel = wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<std::vector<int>>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireWireSoA", file);
    // --- Parallel Fill Function ---
    auto fillFunc = [&](int start, int end) {
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
        for (int i = start; i < end; ++i) {
            HitSoA hit = generateRandomHitSoA(i, fieldSize);
            WireSoA wire = generateRandomWireSoA(i, fieldSize);
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
    int chunk = eventCount / nThreads;
    std::vector<std::thread> threads;
    for (int t = 0; t < nThreads; ++t) {
        int start = t * chunk;
        int end = (t == nThreads - 1) ? eventCount : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSplitHitAndWireDataSoA(int eventCount, int fieldSize, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;

    // Ensure output directory exists
    std::filesystem::create_directories("./hitwire");

    // Open the existing file in UPDATE mode
    TFile file(fileName.c_str(), "UPDATE");

    // --- HIT NTUPLE ---
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID_hit = hitModel->MakeField<long long>("EventID");
    auto fChannel = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    auto fView = hitModel->MakeField<std::vector<int>>("View");
    auto fStartTick = hitModel->MakeField<std::vector<int>>("StartTick");
    auto fEndTick = hitModel->MakeField<std::vector<int>>("EndTick");
    auto fPeakTime = hitModel->MakeField<std::vector<float>>("PeakTime");
    auto fSigmaPeakTime = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    auto fRMS = hitModel->MakeField<std::vector<float>>("RMS");
    auto fPeakAmplitude = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    auto fSigmaPeakAmplitude = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    auto fROISummedADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    auto fHitSummedADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    auto fIntegral = hitModel->MakeField<std::vector<float>>("Integral");
    auto fSigmaIntegral = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    auto fMultiplicity = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    auto fLocalIndex = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    auto fGoodnessOfFit = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    auto fNDF = hitModel->MakeField<std::vector<int>>("NDF");
    auto fSignalType = hitModel->MakeField<std::vector<int>>("SignalType");
    auto fWireID_Cryostat = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    auto fWireID_TPC = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    auto fWireID_Plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    auto fWireID_Wire = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitNTuple", file);

    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_wire = wireModel->MakeField<long long>("EventID");
    auto fWire_Channel = wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<std::vector<int>>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireNTuple", file);

    // --- Parallel Fill Function ---
    auto fillFunc = [&](int start, int end) {
        // Each thread gets its own fill context and entry for both ntuples
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();

        for (int i = start; i < end; ++i) {
            // Generate data
            HitSoA h = generateRandomHitSoA(i, fieldSize);
            WireSoA w = generateRandomWireSoA(i, fieldSize);

            // Set hit fields
            *eventID_hit = h.EventID;
            *fChannel = h.getChannel();
            *fView = h.getView();
            *fStartTick = h.getStartTick();
            *fEndTick = h.getEndTick();
            *fPeakTime = h.getPeakTime();
            *fSigmaPeakTime = h.getSigmaPeakTime();
            *fRMS = h.getRMS();
            *fPeakAmplitude = h.getPeakAmplitude();
            *fSigmaPeakAmplitude = h.getSigmaPeakAmplitude();
            *fROISummedADC = h.getROISummedADC();
            *fHitSummedADC = h.getHitSummedADC();
            *fIntegral = h.getIntegral();
            *fSigmaIntegral = h.getSigmaIntegral();
            *fMultiplicity = h.getMultiplicity();
            *fLocalIndex = h.getLocalIndex();
            *fGoodnessOfFit = h.getGoodnessOfFit();
            *fNDF = h.getNDF();
            *fSignalType = h.getSignalType();
            *fWireID_Cryostat = h.getWireID_Cryostat();
            *fWireID_TPC = h.getWireID_TPC();
            *fWireID_Plane = h.getWireID_Plane();
            *fWireID_Wire = h.getWireID_Wire();
            hitFillContext->Fill(*hitEntry);

            // Set wire fields
            *eventID_wire = w.EventID;
            *fWire_Channel = w.getWire_Channel();
            *fWire_View = w.getWire_View();
            *fSignalROI_nROIs = w.getSignalROI_nROIs();
            *fSignalROI_offsets = w.getSignalROI_offsets();
            *fSignalROI_data = w.getSignalROI_data();
            wireFillContext->Fill(*wireEntry);
        }
    };

    // --- Thread Management ---
    int nThreads = std::thread::hardware_concurrency();
    int chunk = eventCount / nThreads;
    std::vector<std::thread> threads;
    for (int t = 0; t < nThreads; ++t) {
        int start = t * chunk;
        int end = (t == nThreads - 1) ? eventCount : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSpilHitAndWireDataSoA(int eventCount, int spilCount, int fieldSize, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;

    int adjustedFieldSize = fieldSize / spilCount;
    std::filesystem::create_directories("./hitwire");
    std::filesystem::create_directories("./hitwire/hitspils");
    std::filesystem::create_directories("./hitwire/wirespils");

    // Open the existing file in UPDATE mode
    TFile file(fileName.c_str(), "UPDATE");

    // --- HIT NTUPLE ---
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID_f = hitModel->MakeField<long long>("EventID");
    auto spilID_f = hitModel->MakeField<int>("SpilID");
    auto fChannel = hitModel->MakeField<std::vector<unsigned int>>("Channel");
    auto fView = hitModel->MakeField<std::vector<int>>("View");
    auto fStartTick = hitModel->MakeField<std::vector<int>>("StartTick");
    auto fEndTick = hitModel->MakeField<std::vector<int>>("EndTick");
    auto fPeakTime = hitModel->MakeField<std::vector<float>>("PeakTime");
    auto fSigmaPeakTime = hitModel->MakeField<std::vector<float>>("SigmaPeakTime");
    auto fRMS = hitModel->MakeField<std::vector<float>>("RMS");
    auto fPeakAmplitude = hitModel->MakeField<std::vector<float>>("PeakAmplitude");
    auto fSigmaPeakAmplitude = hitModel->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    auto fROISummedADC = hitModel->MakeField<std::vector<float>>("ROISummedADC");
    auto fHitSummedADC = hitModel->MakeField<std::vector<float>>("HitSummedADC");
    auto fIntegral = hitModel->MakeField<std::vector<float>>("Integral");
    auto fSigmaIntegral = hitModel->MakeField<std::vector<float>>("SigmaIntegral");
    auto fMultiplicity = hitModel->MakeField<std::vector<short int>>("Multiplicity");
    auto fLocalIndex = hitModel->MakeField<std::vector<short int>>("LocalIndex");
    auto fGoodnessOfFit = hitModel->MakeField<std::vector<float>>("GoodnessOfFit");
    auto fNDF = hitModel->MakeField<std::vector<int>>("NDF");
    auto fSignalType = hitModel->MakeField<std::vector<int>>("SignalType");
    auto fWireID_Cryostat = hitModel->MakeField<std::vector<int>>("WireID_Cryostat");
    auto fWireID_TPC = hitModel->MakeField<std::vector<int>>("WireID_TPC");
    auto fWireID_Plane = hitModel->MakeField<std::vector<int>>("WireID_Plane");
    auto fWireID_Wire = hitModel->MakeField<std::vector<int>>("WireID_Wire");
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitSpilSoA", file);

    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_w = wireModel->MakeField<long long>("EventID");
    auto spilID_w = wireModel->MakeField<int>("SpilID");
    auto fWire_Channel = wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<std::vector<int>>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireSpilSoA", file);

    // --- Parallel Fill Function ---
    int total = eventCount * spilCount;
    auto fillFunc = [&](int start, int end) {
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
        for (int idx = start; idx < end; ++idx) {
            int eventID = idx / spilCount;
            int spilID = idx % spilCount;
            long long uniqueEventID = static_cast<long long>(eventID) * 10000 + spilID;
            HitSoA hit = generateRandomHitSoA(uniqueEventID, adjustedFieldSize);
            WireSoA wire = generateRandomWireSoA(uniqueEventID, adjustedFieldSize);
            *eventID_f = hit.EventID;
            *spilID_f = spilID;
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
            *spilID_w = spilID;
            *fWire_Channel = wire.getWire_Channel();
            *fWire_View = wire.getWire_View();
            *fSignalROI_nROIs = wire.getSignalROI_nROIs();
            *fSignalROI_offsets = wire.getSignalROI_offsets();
            *fSignalROI_data = wire.getSignalROI_data();
            wireFillContext->Fill(*wireEntry);
        }
    };
    // --- Thread Management ---
    int nThreads = std::thread::hardware_concurrency();
    int chunk = total / nThreads;
    std::vector<std::thread> threads;
    for (int t = 0; t < nThreads; ++t) {
        int start = t * chunk;
        int end = (t == nThreads - 1) ? total : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteHitWireDataAoS(int eventCount, int fieldSize, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;
    std::filesystem::create_directories("./hitwire");
    TFile file(fileName.c_str(), "UPDATE");
    // --- HIT NTUPLE ---
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitWireAoS", file);
    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_w = wireModel->MakeField<long long>("EventID");
    auto fWire_Channel = wireModel->MakeField<unsigned int>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<int>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<unsigned int>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::size_t>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<float>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireAoS", file);
    // --- Parallel Fill Function ---
    auto fillFunc = [&](int start, int end) {
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
        for (int i = start; i < end; ++i) {
            HitAoS hit = generateRandomHitAoS(i);
            WireAoS wire = generateRandomWireAoS(i);
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
            hitFillContext->Fill(*hitEntry);
            *eventID_w = wire.EventID;
            *fWire_Channel = wire.fWire_Channel;
            *fWire_View = wire.fWire_View;
            *fSignalROI_nROIs = wire.fSignalROI_nROIs;
            *fSignalROI_offsets = wire.fSignalROI_offsets;
            *fSignalROI_data = wire.fSignalROI_data;
            wireFillContext->Fill(*wireEntry);
        }
    };
    int nThreads = std::thread::hardware_concurrency();
    int chunk = eventCount / nThreads;
    std::vector<std::thread> threads;
    for (int t = 0; t < nThreads; ++t) {
        int start = t * chunk;
        int end = (t == nThreads - 1) ? eventCount : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSplitHitAndWireDataAoS(int eventCount, int fieldSize, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;

    // Ensure output directory exists
    std::filesystem::create_directories("./hitwire");

    // Open the existing file in UPDATE mode
    TFile file(fileName.c_str(), "UPDATE");

    // --- HIT NTUPLE ---
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID_hit = hitModel->MakeField<long long>("EventID");
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitAoS", file);

    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_wire = wireModel->MakeField<long long>("EventID");
    auto fWire_Channel = wireModel->MakeField<unsigned int>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<int>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<unsigned int>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::size_t>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<float>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireAoS", file);

    // --- Parallel Fill Function ---
    auto fillFunc = [&](int start, int end) {
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
        for (int i = start; i < end; ++i) {
            HitAoS h = generateRandomHitAoS(i);
            WireAoS w = generateRandomWireAoS(i);
            // Set hit fields
            *eventID_hit = h.EventID;
            *fChannel = h.fChannel;
            *fView = h.fView;
            *fStartTick = h.fStartTick;
            *fEndTick = h.fEndTick;
            *fPeakTime = h.fPeakTime;
            *fSigmaPeakTime = h.fSigmaPeakTime;
            *fRMS = h.fRMS;
            *fPeakAmplitude = h.fPeakAmplitude;
            *fSigmaPeakAmplitude = h.fSigmaPeakAmplitude;
            *fROISummedADC = h.fROISummedADC;
            *fHitSummedADC = h.fHitSummedADC;
            *fIntegral = h.fIntegral;
            *fSigmaIntegral = h.fSigmaIntegral;
            *fMultiplicity = h.fMultiplicity;
            *fLocalIndex = h.fLocalIndex;
            *fGoodnessOfFit = h.fGoodnessOfFit;
            *fNDF = h.fNDF;
            *fSignalType = h.fSignalType;
            *fWireID_Cryostat = h.fWireID_Cryostat;
            *fWireID_TPC = h.fWireID_TPC;
            *fWireID_Plane = h.fWireID_Plane;
            *fWireID_Wire = h.fWireID_Wire;
            hitFillContext->Fill(*hitEntry);
            // Set wire fields
            *eventID_wire = w.EventID;
            *fWire_Channel = w.fWire_Channel;
            *fWire_View = w.fWire_View;
            *fSignalROI_nROIs = w.fSignalROI_nROIs;
            *fSignalROI_offsets = w.fSignalROI_offsets;
            *fSignalROI_data = w.fSignalROI_data;
            wireFillContext->Fill(*wireEntry);
        }
    };
    // --- Thread Management ---
    int nThreads = std::thread::hardware_concurrency();
    int chunk = eventCount / nThreads;
    std::vector<std::thread> threads;
    for (int t = 0; t < nThreads; ++t) {
        int start = t * chunk;
        int end = (t == nThreads - 1) ? eventCount : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    // Writers go out of scope and flush/close automatically
}

void generateAndWriteSpilHitAndWireDataAoS(int eventCount, int spilCount, int fieldSize, const std::string& fileName) {
    namespace EXP = ROOT::Experimental;

    int adjustedFieldSize = fieldSize / spilCount;
    std::filesystem::create_directories("./hitwire");
    std::filesystem::create_directories("./hitwire/hitspils");
    std::filesystem::create_directories("./hitwire/wirespils");

    // Open the existing file in UPDATE mode
    TFile file(fileName.c_str(), "UPDATE");

    // --- HIT NTUPLE ---
    auto hitModel = ROOT::RNTupleModel::Create();
    auto eventID_f = hitModel->MakeField<long long>("EventID");
    auto spilID_f = hitModel->MakeField<int>("SpilID");
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
    auto hitWriter = EXP::RNTupleParallelWriter::Append(std::move(hitModel), "HitSpilAoS", file);

    // --- WIRE NTUPLE ---
    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_w = wireModel->MakeField<long long>("EventID");
    auto spilID_w = wireModel->MakeField<int>("SpilID");
    auto fWire_Channel = wireModel->MakeField<unsigned int>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<int>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<unsigned int>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::size_t>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<float>("SignalROI_data");
    auto wireWriter = EXP::RNTupleParallelWriter::Append(std::move(wireModel), "WireSpilAoS", file);

    // --- Parallel Fill Function ---
    int total = eventCount * spilCount;
    auto fillFunc = [&](int start, int end) {
        auto hitFillContext = hitWriter->CreateFillContext();
        auto hitEntry = hitFillContext->CreateEntry();
        auto wireFillContext = wireWriter->CreateFillContext();
        auto wireEntry = wireFillContext->CreateEntry();
        for (int idx = start; idx < end; ++idx) {
            int eventID = idx / spilCount;
            int spilID = idx % spilCount;
            long long uniqueEventID = static_cast<long long>(eventID) * 10000 + spilID;
            for (int i = 0; i < adjustedFieldSize; ++i) {
                HitAoS hit = generateRandomHitAoS(uniqueEventID);
                WireAoS wire = generateRandomWireAoS(uniqueEventID);
                *eventID_f = hit.EventID;
                *spilID_f = spilID;
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
                hitFillContext->Fill(*hitEntry);
                *eventID_w = wire.EventID;
                *spilID_w = spilID;
                *fWire_Channel = wire.fWire_Channel;
                *fWire_View = wire.fWire_View;
                *fSignalROI_nROIs = wire.fSignalROI_nROIs;
                *fSignalROI_offsets = wire.fSignalROI_offsets;
                *fSignalROI_data = wire.fSignalROI_data;
                wireFillContext->Fill(*wireEntry);
            }
        }
    };
    // --- Thread Management ---
    int nThreads = std::thread::hardware_concurrency();
    int chunk = total / nThreads;
    std::vector<std::thread> threads;
    for (int t = 0; t < nThreads; ++t) {
        int start = t * chunk;
        int end = (t == nThreads - 1) ? total : start + chunk;
        threads.emplace_back(fillFunc, start, end);
    }
    for (auto& th : threads) th.join();
    // Writers go out of scope and flush/close automatically
} 
