#include "HitWireWriters.hpp"
#include "Hit.hpp"
#include "Wire.hpp"
#include "HitWireGenerators.hpp"
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>
#include <TFile.h>
#include <iostream>
#include <filesystem>

void generateAndWriteHitWireDataSoA(int eventCount, int fieldSize) {
    // Prepare RNTuple model
    auto model = ROOT::RNTupleModel::Create();
    auto eventID = model->MakeField<long long>("EventID");
    auto fChannel = model->MakeField<std::vector<unsigned int>>("Channel");
    auto fView = model->MakeField<std::vector<int>>("View");
    auto fStartTick = model->MakeField<std::vector<int>>("StartTick");
    auto fEndTick = model->MakeField<std::vector<int>>("EndTick");
    auto fPeakTime = model->MakeField<std::vector<float>>("PeakTime");
    auto fSigmaPeakTime = model->MakeField<std::vector<float>>("SigmaPeakTime");
    auto fRMS = model->MakeField<std::vector<float>>("RMS");
    auto fPeakAmplitude = model->MakeField<std::vector<float>>("PeakAmplitude");
    auto fSigmaPeakAmplitude = model->MakeField<std::vector<float>>("SigmaPeakAmplitude");
    auto fROISummedADC = model->MakeField<std::vector<float>>("ROISummedADC");
    auto fHitSummedADC = model->MakeField<std::vector<float>>("HitSummedADC");
    auto fIntegral = model->MakeField<std::vector<float>>("Integral");
    auto fSigmaIntegral = model->MakeField<std::vector<float>>("SigmaIntegral");
    auto fMultiplicity = model->MakeField<std::vector<short int>>("Multiplicity");
    auto fLocalIndex = model->MakeField<std::vector<short int>>("LocalIndex");
    auto fGoodnessOfFit = model->MakeField<std::vector<float>>("GoodnessOfFit");
    auto fNDF = model->MakeField<std::vector<int>>("NDF");
    auto fSignalType = model->MakeField<std::vector<int>>("SignalType");
    auto fWireID_Cryostat = model->MakeField<std::vector<int>>("WireID_Cryostat");
    auto fWireID_TPC = model->MakeField<std::vector<int>>("WireID_TPC");
    auto fWireID_Plane = model->MakeField<std::vector<int>>("WireID_Plane");
    auto fWireID_Wire = model->MakeField<std::vector<int>>("WireID_Wire");
    // wire fields
    auto fWire_Channel = model->MakeField<std::vector<unsigned int>>("Wire_Channel");
    auto fWire_View = model->MakeField<std::vector<int>>("Wire_View");
    // ROI flattening
    auto fSignalROI_nROIs = model->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    auto fSignalROI_offsets = model->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    auto fSignalROI_data = model->MakeField<std::vector<float>>("SignalROI_data");

    // Ensure output directory exists using std::filesystem
    std::filesystem::create_directories("./hitwire");

    auto file = TFile::Open("./hitwire/HitWire.root", "RECREATE");
    auto writer = ROOT::RNTupleWriter::Append(std::move(model), "HitWire", *file);
    for (int i = 0; i < eventCount; ++i) {
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
        *fWire_Channel = wire.getWire_Channel();
        *fWire_View = wire.getWire_View();
        *fSignalROI_nROIs = wire.getSignalROI_nROIs();
        *fSignalROI_offsets = wire.getSignalROI_offsets();
        *fSignalROI_data = wire.getSignalROI_data();
        writer->Fill();
    }
    writer->CommitCluster();
    std::cout << "HitWire RNTuple written to ./hitwire/HitWire.root" << std::endl;
    writer->CommitDataset();
    file->Close();
    delete file;
}

void generateAndWriteSplitHitAndWireDataSoA(int eventCount, int fieldSize) {
    // Prepare Hit RNTuple writer
    auto file = TFile::Open("./hitwire/split_hitwire.root", "RECREATE");

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
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "HitNTuple", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_wire = wireModel->MakeField<long long>("EventID");
    auto fWire_Channel = wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<std::vector<int>>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "WireNTuple", *file);

    for (int i = 0; i < eventCount; ++i) {
        HitSoA h = generateRandomHitSoA(i, fieldSize);
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
        hitWriter->Fill();

        WireSoA w = generateRandomWireSoA(i, fieldSize);
        *eventID_wire = w.EventID;
        *fWire_Channel = w.getWire_Channel();
        *fWire_View = w.getWire_View();
        *fSignalROI_nROIs = w.getSignalROI_nROIs();
        *fSignalROI_offsets = w.getSignalROI_offsets();
        *fSignalROI_data = w.getSignalROI_data();
        wireWriter->Fill();
    }
    hitWriter->CommitCluster();
    hitWriter->CommitDataset();
    wireWriter->CommitCluster();
    wireWriter->CommitDataset();
    std::cout << "Hit and Wire RNTuples written to ./hitwire/split_hitwire.root" << std::endl;
    file->Close();
    delete file;
}

void generateAndWriteSpilHitAndWireDataSoA(int eventCount, int spilCount, int fieldSize) {

    int adjustedFieldSize = fieldSize / spilCount;
    // Create directories if they do not exist using std::filesystem
    std::filesystem::create_directories("./hitwire");
    std::filesystem::create_directories("./hitwire/hitspils");
    std::filesystem::create_directories("./hitwire/wirespils");

    // Open shared files and writers for all hits and wires
    auto hitFile = TFile::Open("./hitwire/hitspils/hits_spil_all.root", "RECREATE");
    auto wireFile = TFile::Open("./hitwire/wirespils/wires_spil_all.root", "RECREATE");

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
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "HitSpil", *hitFile);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_w = wireModel->MakeField<long long>("EventID");
    auto spilID_w = wireModel->MakeField<int>("SpilID");
    auto fWire_Channel = wireModel->MakeField<std::vector<unsigned int>>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<std::vector<int>>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<std::vector<unsigned int>>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::vector<std::size_t>>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<std::vector<float>>("SignalROI_data");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "WireSpil", *wireFile);

    for (int eventID = 0; eventID < eventCount; ++eventID) {
        for (int spilID = 0; spilID < spilCount; ++spilID) {
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
            hitWriter->Fill();

            *eventID_w = wire.EventID;
            *spilID_w = spilID;
            *fWire_Channel = wire.getWire_Channel();
            *fWire_View = wire.getWire_View();
            *fSignalROI_nROIs = wire.getSignalROI_nROIs();
            *fSignalROI_offsets = wire.getSignalROI_offsets();
            *fSignalROI_data = wire.getSignalROI_data();
            wireWriter->Fill();
        }
    }
    hitWriter->CommitCluster();
    hitWriter->CommitDataset();
    wireWriter->CommitCluster();
    wireWriter->CommitDataset();
    std::cout << "Hit and Wire Spil RNTuples written to ./hitwire/hitspils/hits_spil_all.root and ./hitwire/wirespils/wires_spil_all.root" << std::endl;
    hitFile->Close();
    wireFile->Close();
    delete hitFile;
    delete wireFile;
}

void generateAndWriteHitWireDataAoS(int eventCount, int fieldSize) {
    auto file = TFile::Open("./hitwire/HitWireAoS.root", "RECREATE");
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
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "HitWireAoS", *file);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_w = wireModel->MakeField<long long>("EventID");
    auto fWire_Channel = wireModel->MakeField<unsigned int>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<int>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<unsigned int>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::size_t>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<float>("SignalROI_data");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "WireAoS", *file);

    for (int i = 0; i < eventCount; ++i) {
        for (int j = 0; j < fieldSize; ++j) {
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
            hitWriter->Fill();

            *eventID_w = wire.EventID;
            *fWire_Channel = wire.fWire_Channel;
            *fWire_View = wire.fWire_View;
            *fSignalROI_nROIs = wire.fSignalROI_nROIs;
            *fSignalROI_offsets = wire.fSignalROI_offsets;
            *fSignalROI_data = wire.fSignalROI_data;
            wireWriter->Fill();
        }
    }
    hitWriter->CommitCluster();
    hitWriter->CommitDataset();
    wireWriter->CommitCluster();
    wireWriter->CommitDataset();
    std::cout << "HitWireAoS RNTuple written to ./hitwire/HitWireAoS.root" << std::endl;
    file->Close();
    delete file;
}

void generateAndWriteSplitHitAndWireDataAoS(int eventCount, int fieldSize) {
    auto file = TFile::Open("./hitwire/split_hitwireAoS.root", "RECREATE");
    // Hit dataset
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
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "HitAoS", *file);
    // Wire dataset
    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_wire = wireModel->MakeField<long long>("EventID");
    auto fWire_Channel = wireModel->MakeField<unsigned int>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<int>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<unsigned int>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::size_t>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<float>("SignalROI_data");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "WireAoS", *file);
    
    // Write hits
    for (int i = 0; i < eventCount; ++i) {
        for (int j = 0; j < fieldSize; ++j) {
            HitAoS h = generateRandomHitAoS(i);
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
            hitWriter->Fill();
        }
    }
    hitWriter->CommitCluster();
    hitWriter->CommitDataset();
    
    // Write wires
    for (int i = 0; i < eventCount; ++i) {
        for (int j = 0; j < fieldSize; ++j) {
            WireAoS w = generateRandomWireAoS(i);
            *eventID_wire = w.EventID;
            *fWire_Channel = w.fWire_Channel;
            *fWire_View = w.fWire_View;
            *fSignalROI_nROIs = w.fSignalROI_nROIs;
            *fSignalROI_offsets = w.fSignalROI_offsets;
            *fSignalROI_data = w.fSignalROI_data;
            wireWriter->Fill();
        }
    }
    wireWriter->CommitCluster();
    wireWriter->CommitDataset();
    std::cout << "Split HitAoS and WireAoS RNTuples written to ./hitwire/split_hitwireAoS.root" << std::endl;
    file->Close();
    delete file;
}

void generateAndWriteSpilHitAndWireDataAoS(int eventCount, int spilCount, int fieldSize) {
    int adjustedFieldSize = fieldSize / spilCount;
    
    // Create directories if they do not exist using std::filesystem
    std::filesystem::create_directories("./hitwire");
    std::filesystem::create_directories("./hitwire/hitspils");
    std::filesystem::create_directories("./hitwire/wirespils");

    // Open shared files and writers for all hits and wires
    auto hitFile = TFile::Open("./hitwire/hitspils/hits_spil_all_AoS.root", "RECREATE");
    auto wireFile = TFile::Open("./hitwire/wirespils/wires_spil_all_AoS.root", "RECREATE");

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
    auto hitWriter = ROOT::RNTupleWriter::Append(std::move(hitModel), "HitSpilAoS", *hitFile);

    auto wireModel = ROOT::RNTupleModel::Create();
    auto eventID_w = wireModel->MakeField<long long>("EventID");
    auto spilID_w = wireModel->MakeField<int>("SpilID");
    auto fWire_Channel = wireModel->MakeField<unsigned int>("Wire_Channel");
    auto fWire_View = wireModel->MakeField<int>("Wire_View");
    auto fSignalROI_nROIs = wireModel->MakeField<unsigned int>("SignalROI_nROIs");
    auto fSignalROI_offsets = wireModel->MakeField<std::size_t>("SignalROI_offsets");
    auto fSignalROI_data = wireModel->MakeField<float>("SignalROI_data");
    auto wireWriter = ROOT::RNTupleWriter::Append(std::move(wireModel), "WireSpilAoS", *wireFile);

    for (int eventID = 0; eventID < eventCount; ++eventID) {
        for (int spilID = 0; spilID < spilCount; ++spilID) {
            for (int i = 0; i < adjustedFieldSize; ++i) {
                long long uniqueEventID = static_cast<long long>(eventID) * 10000 + spilID;
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
                hitWriter->Fill();

                *eventID_w = wire.EventID;
                *spilID_w = spilID;
                *fWire_Channel = wire.fWire_Channel;
                *fWire_View = wire.fWire_View;
                *fSignalROI_nROIs = wire.fSignalROI_nROIs;
                *fSignalROI_offsets = wire.fSignalROI_offsets;
                *fSignalROI_data = wire.fSignalROI_data;
                wireWriter->Fill();
            }
        }
    }
    hitWriter->CommitCluster();
    hitWriter->CommitDataset();
    hitFile->Close();
    delete hitFile;
    wireWriter->CommitCluster();
    wireWriter->CommitDataset();
    wireFile->Close();
    delete wireFile;
    std::cout << "Hit and Wire Spil AoS RNTuples written to ./hitwire/hitspils/hits_spil_all_AoS.root and ./hitwire/wirespils/wires_spil_all_AoS.root" << std::endl;
} 