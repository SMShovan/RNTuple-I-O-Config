#include "HitWireWriters.hpp"
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
#include <utility>
#include <TObject.h>
#include <mutex>
#include <numeric>
#include <cmath>
#include <iomanip> // For table formatting
#include <map>
#include <vector>
#include <utility> // for std::pair

// NOTE: Adding experimental includes for parallel writing
#include <ROOT/RRawPtrWriteEntry.hxx>
#include <ROOT/RFieldToken.hxx>
#include <ROOT/RNTupleFillContext.hxx>
#include <ROOT/RNTupleFillStatus.hxx>
#include <ROOT/RNTupleParallelWriter.hxx>
#include <ROOT/RNTupleWriteOptions.hxx>

// Import classes from Experimental namespace
using ROOT::Experimental::RNTupleFillContext;

using ROOT::Experimental::Detail::RRawPtrWriteEntry;

#include <TROOT.h>

// Add include at the top (after existing includes)
#include "HitWireWriterHelpers.hpp"

#include "WriterResult.hpp"

// Removed static int get_nthreads() as part of refactor step 1

static const std::string kOutputDir = "./output";

// Refactor executeInParallel to accept nThreads as a parameter
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

double generateAndWrite_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_Hit_Wire_Vector\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitResult = CreateHitVectorModelAndToken();
    auto hitModel = std::move(hitResult.first);
    auto hitToken = hitResult.second;

    auto wireResult = CreateWireVectorModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Existing writer creation
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Existing nThreads and contexts/entries init
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

    // Thin lambda
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunHitWireVectorWorkFunc(first, last, seed,
                                        *hitContexts[th], *hitEntries[th],
                                        *wireContexts[th], *wireEntries[th],
                                        hitToken, wireToken, mutex,
                                        numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_VertiSplit_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_VertiSplit_Hit_Wire_Vector\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // After options setup
    auto hitResult = CreateVertiSplitHitModelAndTokens();
    auto hitModel = std::move(hitResult.first);
    auto hitTokens = hitResult.second;

    auto wireResult = CreateWireModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Existing writer creation remains
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Existing nThreads and vector init
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

    // Thin wrapper for executeInParallel
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunVertiSplitWorkFunc(first, last, seed,
                                     *hitContexts[th], *hitEntries[th],
                                     *wireContexts[th], *wireEntries[th],
                                     hitTokens, wireToken, mutex,
                                     numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_HoriSpill_Hit_Wire_Vector(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0 || numHoriSpills <= 0) { std::cerr << "Invalid parameters in generateAndWrite_HoriSpill_Hit_Wire_Vector\n"; return 0.0; }
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    auto [hitModel, hitTokens] = CreateHoriSpillHitModelAndTokens();
    auto wireResult = CreateWireVectorModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Writers
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // nThreads and contexts/entries
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunHoriSpillWorkFunc(first, last, seed,
                                    *hitContexts[th], *hitEntries[th],
                                    *wireContexts[th], *wireEntries[th],
                                    hitTokens, wireToken, mutex,
                                    totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_Hit_Wire_Individual\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Use helpers for model and token creation
    auto hitResult = CreateIndividualHitModelAndTokens();
    auto hitModel = std::move(hitResult.first);
    auto hitTokens = hitResult.second;

    auto wireResult = CreateIndividualWireModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Writers
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // nThreads and contexts/entries
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

    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunIndividualWorkFunc(first, last, seed,
                                     *hitContexts[th], *hitEntries[th],
                                     *wireContexts[th], *wireEntries[th],
                                     hitTokens, wireToken, mutex,
                                     numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_VertiSplit_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_VertiSplit_Hit_Wire_Individual\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    // Use helpers for model and token creation
    auto hitResult = CreateVertiSplitIndividualHitModelAndTokens();
    auto hitModel = std::move(hitResult.first);
    auto hitTokens = hitResult.second;

    auto wireResult = CreateIndividualWireModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Writers
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

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

    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunVertiSplitIndividualWorkFunc(first, last, seed,
                                               *hitContexts[th], *hitEntries[th],
                                               *wireContexts[th], *wireEntries[th],
                                               hitTokens, wireToken, mutex,
                                               numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_HoriSpill_Hit_Wire_Individual(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0 || numHoriSpills <= 0) { std::cerr << "Invalid parameters in generateAndWrite_HoriSpill_Hit_Wire_Individual\n"; return 0.0; }
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    auto hitResult = CreateHoriSpillIndividualHitModelAndTokens();
    auto hitModel = std::move(hitResult.first);
    auto hitTokens = hitResult.second;
    auto wireResult = CreateIndividualWireModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Writers
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

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
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunHoriSpillIndividualWorkFunc(first, last, seed,
                                              *hitContexts[th], *hitEntries[th],
                                              *wireContexts[th], *wireEntries[th],
                                              hitTokens, wireToken, mutex,
                                              totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_Hit_Wire_Vector_Dict\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Hit model and token
    auto hitResult = CreateHitVectorModelAndToken();
    auto hitModel = std::move(hitResult.first);
    auto hitToken = hitResult.second;

    auto wireResult = CreateWireVectorModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Existing writer creation
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // Existing nThreads and contexts/entries init
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

    // Thin lambda
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunHitWireVectorWorkFunc(first, last, seed,
                                        *hitContexts[th], *hitEntries[th],
                                        *wireContexts[th], *wireEntries[th],
                                        hitToken, wireToken, mutex,
                                        numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_Hit_Wire_Individual_Dict\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    // Set up write options for buffered writing
    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024); // 2 MiB for demonstration

    // Use dict-specific helpers for model and token creation
    auto hitResult = CreateDictIndividualHitModelAndToken();
    auto hitModel = std::move(hitResult.first);
    auto hitToken = hitResult.second;

    auto wireResult = CreateDictIndividualWireModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    // Writers
    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

    // nThreads and contexts/entries
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

    // Thin lambda for dict work func
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunDictIndividualWorkFunc(first, last, seed,
                                         *hitContexts[th], *hitEntries[th],
                                         *wireContexts[th], *wireEntries[th],
                                         hitToken, wireToken, mutex,
                                         numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    auto hitResult = CreateDictVertiSplitHitModelAndToken();
    auto hitModel = std::move(hitResult.first);
    auto hitToken = hitResult.second;

    auto wireResult = CreateWireVectorModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

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

    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunDictVertiSplitWorkFunc(first, last, seed,
                                         *hitContexts[th], *hitEntries[th],
                                         *wireContexts[th], *wireEntries[th],
                                         hitToken, wireToken, mutex,
                                         numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    auto hitResult = CreateDictVertiSplitIndividualHitModelAndToken();
    auto hitModel = std::move(hitResult.first);
    auto hitToken = hitResult.second;

    auto wireResult = CreateDictIndividualWireModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

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

    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunDictVertiSplitIndividualWorkFunc(first, last, seed,
                                                   *hitContexts[th], *hitEntries[th],
                                                   *wireContexts[th], *wireEntries[th],
                                                   hitToken, wireToken, mutex,
                                                   numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0 || numHoriSpills <= 0) { std::cerr << "Invalid parameters in generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict\n"; return 0.0; }
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    auto hitResult = CreateDictHoriSpillHitModelAndToken();
    auto hitModel = std::move(hitResult.first);
    auto hitToken = hitResult.second;

    auto wireResult = CreateWireVectorModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

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
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunDictHoriSpillWorkFunc(first, last, seed,
                                        *hitContexts[th], *hitEntries[th],
                                        *wireContexts[th], *wireEntries[th],
                                        hitToken, wireToken, mutex,
                                        totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0 || numHoriSpills <= 0) { std::cerr << "Invalid parameters in generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict\n"; return 0.0; }
    int adjustedHitsPerEvent = hitsPerEvent / numHoriSpills;
    int adjustedWiresPerEvent = wiresPerEvent / numHoriSpills;
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    auto hitResult = CreateDictHoriSpillIndividualHitModelAndToken();
    auto hitModel = std::move(hitResult.first);
    auto hitToken = hitResult.second;

    auto wireResult = CreateDictIndividualWireModelAndToken();
    auto wireModel = std::move(wireResult.first);
    auto wireToken = wireResult.second;

    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

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
    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunDictHoriSpillIndividualWorkFunc(first, last, seed,
                                                  *hitContexts[th], *hitEntries[th],
                                                  *wireContexts[th], *wireEntries[th],
                                                  hitToken, wireToken, mutex,
                                                  totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

double generateAndWrite_Hit_Wire_Vector_Of_Individuals(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
    if (numEvents <= 0 || nThreads <= 0) { std::cerr << "Invalid parameters in generateAndWrite_Hit_Wire_Vector_Of_Individuals\n"; return 0.0; }
    std::filesystem::create_directories(kOutputDir);
    auto file = std::make_unique<TFile>(fileName.c_str(), "RECREATE");
    std::mutex mutex;

    ROOT::RNTupleWriteOptions options;
    options.SetUseBufferedWrite(true);
    options.SetApproxZippedClusterSize(2 * 1024 * 1024);

    auto hitResult = CreateVectorOfIndividualsHitModelAndTokens();
    auto hitModel = std::move(hitResult.first);
    auto hitTokens = hitResult.second;

    auto wireResult = CreateVectorOfIndividualsWireModelAndTokens();
    auto wireModel = std::move(wireResult.first);
    auto wireTokens = wireResult.second;

    auto hitWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(hitModel), "hits", *file, options);
    auto wireWriter = ROOT::Experimental::RNTupleParallelWriter::Append(std::move(wireModel), "wires", *file, options);

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

    auto thinWorkFunc = [&](int first, int last, unsigned seed, int th) {
        return RunVectorOfIndividualsWorkFunc(first, last, seed,
                                              *hitContexts[th], *hitEntries[th],
                                              *wireContexts[th], *wireEntries[th],
                                              hitTokens, wireTokens, mutex,
                                              numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    return totalTime * 1000;
}

std::vector<WriterResult> out(int nThreads, int iter) {
    int numEvents = 1000;
    int hitsPerEvent = 1000;
    int wiresPerEvent = 100;
    int numHoriSpills = 10;
    int roisPerWire = 10;
    int numRuns = iter;

    std::vector<WriterResult> results;

    auto benchmark = [&](const std::string& label, auto writerFunc, auto&&... args) {
        std::vector<double> times;
        for (int i = 0; i < numRuns; ++i) {
            double t = writerFunc(args...);
            times.push_back(t);
        }
        double avg = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
        double sq_sum = std::inner_product(times.begin(), times.end(), times.begin(), 0.0);
        double stddev = std::sqrt((sq_sum - times.size() * avg * avg) / (times.size() - 1));
        results.push_back({label, avg, stddev});
    };

    benchmark("Hit/Wire Vector", generateAndWrite_Hit_Wire_Vector, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/vector.root", nThreads);
    benchmark("Hit/Wire Individual", generateAndWrite_Hit_Wire_Individual, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/individual.root", nThreads);
    benchmark("VertiSplit-Vector", generateAndWrite_VertiSplit_Hit_Wire_Vector, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_vector.root", nThreads);
    benchmark("VertiSplit-Individual", generateAndWrite_VertiSplit_Hit_Wire_Individual, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_individual.root", nThreads);
    benchmark("HoriSpill-Vector", generateAndWrite_HoriSpill_Hit_Wire_Vector, numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_vector.root", nThreads);
    benchmark("HoriSpill-Individual", generateAndWrite_HoriSpill_Hit_Wire_Individual, numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_individual.root", nThreads);
    benchmark("Vector-Dict", generateAndWrite_Hit_Wire_Vector_Dict, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/vector_dict.root", nThreads);
    benchmark("Individual-Dict", generateAndWrite_Hit_Wire_Individual_Dict, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/individual_dict.root", nThreads);
    benchmark("VertiSplit-Vector-Dict", generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_vector_dict.root", nThreads);
    benchmark("VertiSplit-Individual-Dict", generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_individual_dict.root", nThreads);
    benchmark("HoriSpill-Vector-Dict", generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict, numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_vector_dict.root", nThreads);
    benchmark("HoriSpill-Individual-Dict", generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict, numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_individual_dict.root", nThreads);
    benchmark("Vector-of-Individuals", generateAndWrite_Hit_Wire_Vector_Of_Individuals, numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/vector_of_individuals.root", nThreads);

    // Print table
    std::cout << std::left
              << std::setw(32) << "Writer"
              << std::setw(16) << "Average (ms)"
              << std::setw(16) << "StdDev (ms)" << std::endl;
    std::cout << std::string(64, '-') << std::endl;
    for (const auto& r : results) {
        std::cout << std::left
                  << std::setw(32) << r.label
                  << std::setw(16) << r.avg
                  << std::setw(16) << r.stddev << std::endl;
    }
    std::cout << std::string(64, '-') << std::endl;

    return results;
}

std::map<std::string, std::vector<std::pair<int, double>>> benchmarkScaling(int maxThreads, int iter) {
    std::vector<int> threadCounts;
    for (int t = 1; t <= maxThreads; t *= 2) {
        threadCounts.push_back(t);
    }

    std::map<std::string, std::vector<std::pair<int, double>>> scalingData;

    for (int threads : threadCounts) {
        ROOT::EnableImplicitMT(threads);
        auto results = out(threads, iter);
        for (const auto& res : results) {
            scalingData[res.label].emplace_back(threads, res.avg);
        }
    }

    ROOT::DisableImplicitMT(); // Reset after benchmarking
    return scalingData;
}
