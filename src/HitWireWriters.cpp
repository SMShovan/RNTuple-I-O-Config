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


// Add include at the top (after existing includes)
#include "HitWireWriterHelpers.hpp"

// Removed static int get_nthreads() as part of refactor step 1

static const std::string kOutputDir = "./output";

// Refactor executeInParallel to accept nThreads as a parameter
static double executeInParallel(int totalEvents, int nThreads, const std::function<double(int, int, unsigned)>& workFunc) {
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

void generateAndWrite_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunHitWireVectorWorkFunc(first, last, seed,
                                        *hitContexts[th], *hitEntries[th],
                                        *wireContexts[th], *wireEntries[th],
                                        hitToken, wireToken, mutex,
                                        numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] Hit/Wire Vector ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_VertiSplit_Hit_Wire_Vector(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunVertiSplitWorkFunc(first, last, seed,
                                     *hitContexts[th], *hitEntries[th],
                                     *wireContexts[th], *wireEntries[th],
                                     hitTokens, wireToken, mutex,
                                     numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] VertiSplit-Vector ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Vector(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (totalEntries / nThreads);
        return RunHoriSpillWorkFunc(first, last, seed,
                                    *hitContexts[th], *hitEntries[th],
                                    *wireContexts[th], *wireEntries[th],
                                    hitTokens, wireToken, mutex,
                                    totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] HoriSpill-Vector ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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

    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunIndividualWorkFunc(first, last, seed,
                                     *hitContexts[th], *hitEntries[th],
                                     *wireContexts[th], *wireEntries[th],
                                     hitTokens, wireToken, mutex,
                                     numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] Individual ntuples written in " << totalTime * 1000 << " ms" << std::endl;
}

void generateAndWrite_VertiSplit_Hit_Wire_Individual(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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

    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunVertiSplitIndividualWorkFunc(first, last, seed,
                                               *hitContexts[th], *hitEntries[th],
                                               *wireContexts[th], *wireEntries[th],
                                               hitTokens, wireToken, mutex,
                                               numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] VertiSplit-Individual ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Individual(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (totalEntries / nThreads);
        return RunHoriSpillIndividualWorkFunc(first, last, seed,
                                              *hitContexts[th], *hitEntries[th],
                                              *wireContexts[th], *wireEntries[th],
                                              hitTokens, wireToken, mutex,
                                              totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] HoriSpill-Individual ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunHitWireVectorWorkFunc(first, last, seed,
                                        *hitContexts[th], *hitEntries[th],
                                        *wireContexts[th], *wireEntries[th],
                                        hitToken, wireToken, mutex,
                                        numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] Vector-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunDictIndividualWorkFunc(first, last, seed,
                                         *hitContexts[th], *hitEntries[th],
                                         *wireContexts[th], *wireEntries[th],
                                         hitToken, wireToken, mutex,
                                         numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] Individual-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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

    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunDictVertiSplitWorkFunc(first, last, seed,
                                         *hitContexts[th], *hitEntries[th],
                                         *wireContexts[th], *wireEntries[th],
                                         hitToken, wireToken, mutex,
                                         numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] VertiSplit-Vector-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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

    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunDictVertiSplitIndividualWorkFunc(first, last, seed,
                                                   *hitContexts[th], *hitEntries[th],
                                                   *wireContexts[th], *wireEntries[th],
                                                   hitToken, wireToken, mutex,
                                                   numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] VertiSplit-Individual-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (totalEntries / nThreads);
        return RunDictHoriSpillWorkFunc(first, last, seed,
                                        *hitContexts[th], *hitEntries[th],
                                        *wireContexts[th], *wireEntries[th],
                                        hitToken, wireToken, mutex,
                                        totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] HoriSpill-Vector-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict(int numEvents, int numHoriSpills, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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
    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (totalEntries / nThreads);
        return RunDictHoriSpillIndividualWorkFunc(first, last, seed,
                                                  *hitContexts[th], *hitEntries[th],
                                                  *wireContexts[th], *wireEntries[th],
                                                  hitToken, wireToken, mutex,
                                                  totalEntries, nThreads, numHoriSpills, adjustedHitsPerEvent, adjustedWiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(totalEntries, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] HoriSpill-Individual-Dict ntuples written in " << totalTime * 1000 << " ms\n";
}

void generateAndWrite_Hit_Wire_Vector_Of_Individuals(int numEvents, int hitsPerEvent, int wiresPerEvent, int roisPerWire, const std::string& fileName, int nThreads) {
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

    auto thinWorkFunc = [&](int first, int last, unsigned seed) {
        int th = first / (numEvents / nThreads);
        return RunVectorOfIndividualsWorkFunc(first, last, seed,
                                              *hitContexts[th], *hitEntries[th],
                                              *wireContexts[th], *wireEntries[th],
                                              hitTokens, wireTokens, mutex,
                                              numEvents, nThreads, hitsPerEvent, wiresPerEvent, roisPerWire);
    };

    double totalTime = executeInParallel(numEvents, nThreads, thinWorkFunc);
    std::cout << "[Concurrent] Vector-of-Individuals ntuples written in " << totalTime * 1000 << " ms\n";
}

void out(int nThreads) {
    int numEvents = 1000;
    int hitsPerEvent = 1000;
    int wiresPerEvent = 100;
    int numHoriSpills = 10;
    int roisPerWire = 10;

    std::cout << "Generating HitWire data with Vector format..." << std::endl;
    generateAndWrite_Hit_Wire_Vector(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/vector.root", nThreads);
    std::cout << "Generating HitWire data with Individual format..." << std::endl;
    generateAndWrite_Hit_Wire_Individual(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/individual.root", nThreads);
    std::cout << "Generating VertiSplit HitWire data with Vector format..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Vector(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_vector.root", nThreads);
    std::cout << "Generating VertiSplit HitWire data with Individual format..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Individual(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_individual.root", nThreads);
    std::cout << "Generating HoriSpill HitWire data with Vector format..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Vector(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_vector.root", nThreads);
    std::cout << "Generating HoriSpill HitWire data with Individual format..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Individual(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_individual.root", nThreads);
    //--- DICTIONARY-BASED EXPERIMENTS ---
    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Generating HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWrite_Hit_Wire_Vector_Dict(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/vector_dict.root", nThreads);
    std::cout << "Generating HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWrite_Hit_Wire_Individual_Dict(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/individual_dict.root", nThreads);
    std::cout << "Generating VertiSplit HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Vector_Dict(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_vector_dict.root", nThreads);
    std::cout << "Generating VertiSplit HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWrite_VertiSplit_Hit_Wire_Individual_Dict(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/VertiSplit_individual_dict.root", nThreads);
    std::cout << "Generating HoriSpill HitWire data with Vector format (Dict)..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Vector_Dict(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_vector_dict.root", nThreads);
    std::cout << "Generating HoriSpill HitWire data with Individual format (Dict)..." << std::endl;
    generateAndWrite_HoriSpill_Hit_Wire_Individual_Dict(numEvents, numHoriSpills, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/HoriSpill_individual_dict.root", nThreads);
    std::cout << "Generating HitWire data with Vector of Individuals format..." << std::endl;
    generateAndWrite_Hit_Wire_Vector_Of_Individuals(numEvents, hitsPerEvent, wiresPerEvent, roisPerWire, kOutputDir + "/vector_of_individuals.root", nThreads);
}
