#include "Hit.hpp"
#include "Wire.hpp"
#include "Utils.hpp" // Assuming this exists for utilities like generateSeeds
#include "updatedHitWireWriterHelpers.hpp"
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>
#include <ROOT/RNTupleParallelWriter.hxx>
#include <ROOT/RNTupleWriteOptions.hxx>
#include <TFile.h>
#include <TStopwatch.h>
#include <filesystem>
#include <thread>
#include <future>
#include <vector>
#include <mutex>
#include <numeric>
#include <cmath>
#include <iomanip>
#include <map>
#include <utility>
#include <functional>
#include "WriterResult.hpp"
#include <TROOT.h>

// Move executeInParallel to the top of the file, before any function implementations
static double executeInParallel(int totalEvents, int nThreads, const std::function<double(int, int, unsigned, int)>& workFunc) {
    if (nThreads <= 0 || totalEvents < 0) return 0.0;
    if (totalEvents == 0) return 0.0;
    auto seeds = Utils::generateSeeds(nThreads);
    int chunk = totalEvents / nThreads;
    std::vector<std::future<double>> futures;
    for (int th = 0; th < nThreads; ++th) {
        int start = th * chunk;
        int end = (th == nThreads - 1) ? totalEvents : start + chunk;
        if (start >= end) continue;
        futures.push_back(std::async(std::launch::async, workFunc, start, end, seeds[th], th));
    }
    double totalTime = 0.0;
    for (auto& f : futures) {
        totalTime += f.get();
    }
    return totalTime;
}

// Placeholder for AOS_event_allDataProduct
double AOS_event_allDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    auto [model, token] = CreateAOSAllDataProductModelAndToken();
    auto writer = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(model), "aos_events", *file, options);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> contexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> entries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        contexts[th] = writer->CreateFillContext();
        entries[th] = contexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_event_allDataProductWorkFunc(first, last, seed, *contexts[th], *entries[th], token, mutex, hitsPerEvent, wiresPerEvent, roisPerWire);
    };
    double totalTime = executeInParallel(numEvents, nThreads, workFunc);
    return totalTime * 1000;
}

// Implementation for perDataProduct
double AOS_event_perDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    auto [hitsModel, hitsToken] = CreateAOSHitsModelAndToken();
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "aos_hits", *file, options);
    auto [wiresModel, wiresToken] = CreateAOSWiresModelAndToken();
    auto wiresWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wiresModel), "aos_wires", *file, options);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> hitsEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> wiresContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> wiresEntries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->GetModel().CreateRawPtrWriteEntry();
        wiresContexts[th] = wiresWriter->CreateFillContext();
        wiresEntries[th] = wiresContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_event_perDataProductWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], hitsToken, *wiresContexts[th], *wiresEntries[th], wiresToken, mutex, hitsPerEvent, wiresPerEvent, roisPerWire);
    };
    double totalTime = executeInParallel(numEvents, nThreads, workFunc);
    return totalTime * 1000;
}

// Implementation for perGroup (similar, with added rois writer)
double AOS_event_perGroup(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    auto [hitsModel, hitsToken] = CreateAOSHitsModelAndToken();
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "aos_hits", *file, options);
    auto [wiresModel, wiresToken] = CreateAOSBaseWiresModelAndToken();
    auto wiresWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wiresModel), "aos_wires", *file, options);
    auto [roisModel, roisToken] = CreateAOSROIsModelAndToken();
    auto roisWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(roisModel), "aos_rois", *file, options);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> hitsEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> wiresContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> wiresEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> roisContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> roisEntries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->GetModel().CreateRawPtrWriteEntry();
        wiresContexts[th] = wiresWriter->CreateFillContext();
        wiresEntries[th] = wiresContexts[th]->GetModel().CreateRawPtrWriteEntry();
        roisContexts[th] = roisWriter->CreateFillContext();
        roisEntries[th] = roisContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_event_perGroupWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], hitsToken, *wiresContexts[th], *wiresEntries[th], wiresToken, *roisContexts[th], *roisEntries[th], roisToken, mutex, hitsPerEvent, wiresPerEvent, roisPerWire);
    };
    double totalTime = executeInParallel(numEvents, nThreads, workFunc);
    return totalTime * 1000;
}

// Add any shared helpers here, e.g., executeInParallel from existing code (duplicated to avoid changes)

double AOS_spill_allDataProduct(int numEvents, int numSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    int adjustedHits = hitsPerEvent / numSpills;
    int adjustedWires = wiresPerEvent / numSpills;
    int totalEntries = numEvents * numSpills;
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    auto [model, token] = CreateAOSAllDataProductModelAndToken();
    auto writer = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(model), "aos_spills", *file, options);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> contexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> entries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        contexts[th] = writer->CreateFillContext();
        entries[th] = contexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_spill_allDataProductWorkFunc(first, last, seed, *contexts[th], *entries[th], token, mutex, numSpills, adjustedHits, adjustedWires, roisPerWire);
    };
    double totalTime = executeInParallel(totalEntries, nThreads, workFunc);
    return totalTime * 1000;
}

// Similarly implement AOS_spill_perDataProduct and AOS_spill_perGroup 

double AOS_spill_perDataProduct(int numEvents, int numSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    int adjustedHits = hitsPerEvent / numSpills;
    int adjustedWires = wiresPerEvent / numSpills;
    int totalEntries = numEvents * numSpills;
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    auto [hitsModel, hitsToken] = CreateAOSHitsModelAndToken();
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "aos_spill_hits", *file, options);
    auto [wiresModel, wiresToken] = CreateAOSWiresModelAndToken();
    auto wiresWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wiresModel), "aos_spill_wires", *file, options);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> hitsEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> wiresContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> wiresEntries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->GetModel().CreateRawPtrWriteEntry();
        wiresContexts[th] = wiresWriter->CreateFillContext();
        wiresEntries[th] = wiresContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_spill_perDataProductWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], hitsToken, *wiresContexts[th], *wiresEntries[th], wiresToken, mutex, numSpills, adjustedHits, adjustedWires, roisPerWire);
    };
    double totalTime = executeInParallel(totalEntries, nThreads, workFunc);
    return totalTime * 1000;
}

double AOS_spill_perGroup(int numEvents, int numSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    int adjustedHits = hitsPerEvent / numSpills;
    int adjustedWires = wiresPerEvent / numSpills;
    int totalEntries = numEvents * numSpills;
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    auto [hitsModel, hitsToken] = CreateAOSHitsModelAndToken();
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "aos_spill_hits", *file, options);
    auto [wiresModel, wiresToken] = CreateAOSBaseWiresModelAndToken();
    auto wiresWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wiresModel), "aos_spill_wires", *file, options);
    auto [roisModel, roisToken] = CreateAOSROIsModelAndToken();
    auto roisWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(roisModel), "aos_spill_rois", *file, options);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> hitsEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> wiresContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> wiresEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> roisContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> roisEntries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->GetModel().CreateRawPtrWriteEntry();
        wiresContexts[th] = wiresWriter->CreateFillContext();
        wiresEntries[th] = wiresContexts[th]->GetModel().CreateRawPtrWriteEntry();
        roisContexts[th] = roisWriter->CreateFillContext();
        roisEntries[th] = roisContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_spill_perGroupWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], hitsToken, *wiresContexts[th], *wiresEntries[th], wiresToken, *roisContexts[th], *roisEntries[th], roisToken, mutex, numSpills, adjustedHits, adjustedWires, roisPerWire);
    };
    double totalTime = executeInParallel(totalEntries, nThreads, workFunc);
    return totalTime * 1000;
} 

double AOS_topObject_perDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    // Assume hitsPerEvent == wiresPerEvent
    int totalEntries = numEvents * hitsPerEvent;
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    auto hitsModel = ROOT::RNTupleModel::Create();
    hitsModel->MakeField<HitIndividual>("hit");
    auto hitsToken = hitsModel->GetToken("hit");
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "top_hits", *file, options);
    auto wiresModel = ROOT::RNTupleModel::Create();
    wiresModel->MakeField<WireIndividual>("wire");
    auto wiresToken = wiresModel->GetToken("wire");
    auto wiresWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wiresModel), "top_wires", *file, options);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> hitsEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> wiresContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> wiresEntries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->GetModel().CreateRawPtrWriteEntry();
        wiresContexts[th] = wiresWriter->CreateFillContext();
        wiresEntries[th] = wiresContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_topObject_perDataProductWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], hitsToken, *wiresContexts[th], *wiresEntries[th], wiresToken, mutex, roisPerWire);
    };
    double totalTime = executeInParallel(totalEntries, nThreads, workFunc);
    return totalTime * 1000;
}

double AOS_topObject_perGroup(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    int totalEntries = numEvents * hitsPerEvent;
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);
    // Hits model
    auto hitsModel = ROOT::RNTupleModel::Create();
    hitsModel->MakeField<HitIndividual>("hit");
    auto hitsToken = hitsModel->GetToken("hit");
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "top_hits", *file, options);
    // Wires model (WireBase)
    auto wiresModel = ROOT::RNTupleModel::Create();
    wiresModel->MakeField<WireBase>("wire");
    auto wiresToken = wiresModel->GetToken("wire");
    auto wiresWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wiresModel), "top_wires", *file, options);
    // ROIs model
    auto roisModel = ROOT::RNTupleModel::Create();
    roisModel->MakeField<std::vector<FlatROI>>("rois");
    auto roisToken = roisModel->GetToken("rois");
    auto roisWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(roisModel), "top_rois", *file, options);
    // Contexts and entries
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> hitsEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> wiresContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> wiresEntries(nThreads);
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> roisContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::Experimental::Detail::RRawPtrWriteEntry>> roisEntries(nThreads);
    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->GetModel().CreateRawPtrWriteEntry();
        wiresContexts[th] = wiresWriter->CreateFillContext();
        wiresEntries[th] = wiresContexts[th]->GetModel().CreateRawPtrWriteEntry();
        roisContexts[th] = roisWriter->CreateFillContext();
        roisEntries[th] = roisContexts[th]->GetModel().CreateRawPtrWriteEntry();
    }
    auto workFunc = [&](int first, int last, unsigned seed, int th) {
        return RunAOS_topObject_perGroupWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], hitsToken, *wiresContexts[th], *wiresEntries[th], wiresToken, *roisContexts[th], *roisEntries[th], roisToken, mutex, roisPerWire);
    };
    double totalTime = executeInParallel(totalEntries, nThreads, workFunc);
    return totalTime * 1000;
} 

double AOS_element_perDataProduct(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    // Hits model and writer
    auto hitsModel = ROOT::RNTupleModel::Create();
    auto hitField = hitsModel->MakeField<HitIndividual>("hit");
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "element_hits", *file, options);

    // WireROI model and writer
    auto wireROIModel = ROOT::RNTupleModel::Create();
    auto wireROIField = wireROIModel->MakeField<WireROI>("wire_roi");
    auto wireROIWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireROIModel), "element_wire_rois", *file, options);

    // Contexts and entries for hits
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::REntry>> hitsEntries(nThreads);

    // Contexts and entries for wireROIs
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> wireROIContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::REntry>> wireROIEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->CreateEntry();
        wireROIContexts[th] = wireROIWriter->CreateFillContext();
        wireROIEntries[th] = wireROIContexts[th]->CreateEntry();
    }

    int totalHits = numEvents * hitsPerEvent;
    int totalROIs = numEvents * wiresPerEvent * roisPerWire;

    // Work function for hits (parallel over totalHits)
    auto hitsWorkFunc = [&](int first, int last, unsigned seed, int th) -> double {
        return RunAOS_element_hitsWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], mutex);
    };
    double hitsTime = executeInParallel(totalHits, nThreads, hitsWorkFunc);

    // Work function for wireROIs (parallel over totalROIs)
    auto wireROIWorkFunc = [&](int first, int last, unsigned seed, int th) -> double {
        return RunAOS_element_wireROIWorkFunc(first, last, seed, *wireROIContexts[th], *wireROIEntries[th], mutex, roisPerWire);
    };
    double wireROITime = executeInParallel(totalROIs, nThreads, wireROIWorkFunc);

    return (hitsTime + wireROITime) * 1000; // Total time in ms
}

double AOS_element_perGroup(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    std::filesystem::create_directories("./output");
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    // Hits model and writer (single HitIndividual per row)
    auto hitsModel = ROOT::RNTupleModel::Create();
    auto hitField = hitsModel->MakeField<HitIndividual>("hit");
    auto hitsWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitsModel), "element_hits", *file, options);

    // Wires model and writer (single WireBase per row)
    auto wiresModel = ROOT::RNTupleModel::Create();
    auto wireField = wiresModel->MakeField<WireBase>("wire");
    auto wiresWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wiresModel), "element_wires", *file, options);

    // ROIs model and writer (single FlatROI per row)
    auto roisModel = ROOT::RNTupleModel::Create();
    auto roiField = roisModel->MakeField<FlatROI>("roi");
    auto roisWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(roisModel), "element_rois", *file, options);

    // Contexts and entries
    std::vector<std::shared_ptr<ROOT::Experimental::RNTupleFillContext>> hitsContexts(nThreads), wiresContexts(nThreads), roisContexts(nThreads);
    std::vector<std::unique_ptr<ROOT::REntry>> hitsEntries(nThreads), wiresEntries(nThreads), roisEntries(nThreads);

    for (int th = 0; th < nThreads; ++th) {
        hitsContexts[th] = hitsWriter->CreateFillContext();
        hitsEntries[th] = hitsContexts[th]->CreateEntry();
        wiresContexts[th] = wiresWriter->CreateFillContext();
        wiresEntries[th] = wiresContexts[th]->CreateEntry();
        roisContexts[th] = roisWriter->CreateFillContext();
        roisEntries[th] = roisContexts[th]->CreateEntry();
    }

    int totalHits = numEvents * hitsPerEvent;
    int totalWires = numEvents * wiresPerEvent;
    int totalROIs = numEvents * wiresPerEvent * roisPerWire;

    // Hits work func (parallel over totalHits)
    auto hitsWorkFunc = [&](int first, int last, unsigned seed, int th) -> double {
        return RunAOS_element_hitsWorkFunc(first, last, seed, *hitsContexts[th], *hitsEntries[th], mutex);
    };
    double hitsTime = executeInParallel(totalHits, nThreads, hitsWorkFunc);

    // Wires work func (parallel over totalWires)
    auto wiresWorkFunc = [&](int first, int last, unsigned seed, int th) -> double {
        return RunAOS_element_wiresWorkFunc(first, last, seed, *wiresContexts[th], *wiresEntries[th], mutex);
    };
    double wiresTime = executeInParallel(totalWires, nThreads, wiresWorkFunc);

    // ROIs work func (parallel over totalROIs)
    auto roisWorkFunc = [&](int first, int last, unsigned seed, int th) -> double {
        return RunAOS_element_roisWorkFunc(first, last, seed, *roisContexts[th], *roisEntries[th], mutex, roisPerWire);
    };
    double roisTime = executeInParallel(totalROIs, nThreads, roisWorkFunc);

    return (hitsTime + wiresTime + roisTime) * 1000; // Total time in ms
} 

std::vector<WriterResult> updatedOut(int nThreads, int iter) {
    int numEvents = 1000;
    int hitsPerEvent = 100;
    int wiresPerEvent = 100;
    int roisPerWire = 10;
    int numSpills = 10;
    std::vector<WriterResult> results;
    auto benchmark = [&](const std::string& label, auto func, auto&&... args) {
        std::vector<double> times;
        for (int i = 0; i < iter; ++i) {
            double t = func(args...);
            times.push_back(t);
        }
        double avg = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
        double sq_sum = std::inner_product(times.begin(), times.end(), times.begin(), 0.0);
        double stddev = std::sqrt((sq_sum - times.size() * avg * avg) / (times.size() - 1));
        results.push_back({label, avg, stddev});
    };
    benchmark("AOS_event_allDataProduct", AOS_event_allDataProduct, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_event_all.root", nThreads);
    benchmark("AOS_event_perDataProduct", AOS_event_perDataProduct, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_event_perData.root", nThreads);
    benchmark("AOS_event_perGroup", AOS_event_perGroup, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_event_perGroup.root", nThreads);
    benchmark("AOS_spill_allDataProduct", AOS_spill_allDataProduct, numEvents, numSpills, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_spill_all.root", nThreads);
    benchmark("AOS_spill_perDataProduct", AOS_spill_perDataProduct, numEvents, numSpills, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_spill_perData.root", nThreads);
    benchmark("AOS_spill_perGroup", AOS_spill_perGroup, numEvents, numSpills, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_spill_perGroup.root", nThreads);
    benchmark("AOS_topObject_perDataProduct", AOS_topObject_perDataProduct, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_topObject_perData.root", nThreads);
    benchmark("AOS_topObject_perGroup", AOS_topObject_perGroup, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_topObject_perGroup.root", nThreads);
    benchmark("AOS_element_perDataProduct", AOS_element_perDataProduct, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_element_perData.root", nThreads);
    benchmark("AOS_element_perGroup", AOS_element_perGroup, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, "./output/aos_element_perGroup.root", nThreads);

    std::cout << std::left << std::setw(32) << "Writer" << std::setw(16) << "Average (ms)" << std::setw(16) << "StdDev (ms)" << std::endl;
    for (const auto& res : results) {
        std::cout << std::left << std::setw(32) << res.label << std::setw(16) << res.avg << std::setw(16) << res.stddev << std::endl;
    }
    return results;
} 

std::map<std::string, std::vector<std::pair<int, double>>> benchmarkAOSScaling(int maxThreads, int iter) {
    std::vector<int> threadCounts = {1, 2, 4, 8, 16, 32};
    std::map<std::string, std::vector<std::pair<int, double>>> data;
    for (int threads : threadCounts) {
        ROOT::EnableImplicitMT(threads);
        auto results = updatedOut(threads, iter);
        for (const auto& res : results) {
            data[res.label].emplace_back(threads, res.avg);
        }
    }
    ROOT::DisableImplicitMT();
    return data;
} 