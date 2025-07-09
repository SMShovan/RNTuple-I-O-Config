#include <ROOT/RNTupleReader.hxx>
#include <TStopwatch.h>
#include <future>
#include <thread>
#include <iostream>
#include <vector>
#include "Hit.hpp"

// Helper: get number of threads
static int get_nthreads() {
    int n = std::thread::hardware_concurrency();
    return n > 0 ? n : 4;
}

// Match writers: central output folder
static const std::string kOutputDir = "./output";

// Helper to split entry range
std::vector<std::pair<std::size_t, std::size_t>> split_range(std::size_t begin, std::size_t end, int nChunks) {
    std::vector<std::pair<std::size_t, std::size_t>> chunks;
    std::size_t total = end - begin;
    std::size_t chunkSize = total / nChunks;
    std::size_t remainder = total % nChunks;
    std::size_t current = begin;
    for (int i = 0; i < nChunks; ++i) {
        std::size_t next = current + chunkSize + (i < remainder ? 1 : 0);
        chunks.emplace_back(current, next);
        current = next;
    }
    return chunks;
}

// 1. Vector format (single HitVector column)
void readHitWireDataVector(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();

    // Use one pilot reader to get the entry span, then process chunks in parallel
    auto ntuplePilot = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange  = ntuplePilot->GetEntryRange();

    int nThreads = get_nthreads();
    auto chunks   = split_range(*entryRange.begin(), *entryRange.end(), nThreads);

    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.emplace_back(std::async(std::launch::async, [fileName, chunk]() {
            // Each task opens its own reader (thread-safe pattern recommended by ROOT)
            auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
            auto viewHit = ntuple->GetView<HitVector>("HitVector");
            for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                const HitVector& hit = viewHit(i);
                const auto& probe = hit.getChannel(); // access something to force page load
                volatile auto* sink = &probe;
                (void)sink;
            }
        }));
    }

    for (auto& f : futures) f.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 2. Split Vector format
void readSplitHitAndWireDataVector(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer; timer.Start();

    auto pilot = ROOT::RNTupleReader::Open("hits", fileName);
    auto range = pilot->GetEntryRange();

    int nThreads = get_nthreads();
    auto chunks = split_range(*range.begin(), *range.end(), nThreads);
    std::vector<std::future<void>> futs;
    for(const auto &chunk: chunks){
        futs.emplace_back(std::async(std::launch::async, [fileName, chunk](){
            auto nt = ROOT::RNTupleReader::Open("hits", fileName);
            auto startTick = nt->GetView<std::vector<int>>("StartTick");
            for(std::size_t i=chunk.first;i<chunk.second;++i){
                const auto &v = startTick(i);
                volatile auto* sink=&v; (void)sink;
            }
        }));
    }
    for(auto &f: futs) f.get();
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime()*1000 << " ms" << std::endl;
}

// 3. Spil Vector format
void readSpilHitAndWireDataVector(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    int nThreads = get_nthreads();
    auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.push_back(std::async(std::launch::async, [fileName, chunk]() {
            auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
            auto spilID = ntuple->GetView<int>("SpilID");
            for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                const auto& val = spilID(i);
                volatile auto* ptr = &val;
            }
        }));
    }
    for (auto& f : futures) f.get();
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 4. Individual format
void readHitWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    int nThreads = get_nthreads();
    auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.push_back(std::async(std::launch::async, [fileName, chunk]() {
            auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
            auto channel = ntuple->GetView<unsigned int>("Channel");
            for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                const auto& val = channel(i);
                volatile auto* ptr = &val;
            }
        }));
    }
    for (auto& f : futures) f.get();
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 5. Split Individual format
void readSplitHitAndWireDataIndividual(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    int nThreads = get_nthreads();
    auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.push_back(std::async(std::launch::async, [fileName, chunk]() {
            auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
            auto startTick = ntuple->GetView<int>("StartTick");
            for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                const auto& val = startTick(i);
                volatile auto* ptr = &val;
            }
        }));
    }
    for (auto& f : futures) f.get();
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 6. Spil Individual format
void readSpilHitAndWireDataIndividual(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    int nThreads = get_nthreads();
    auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.push_back(std::async(std::launch::async, [fileName, chunk]() {
            auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
            auto spilID = ntuple->GetView<int>("SpilID");
            for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                const auto& val = spilID(i);
                volatile auto* ptr = &val;
            }
        }));
    }
    for (auto& f : futures) f.get();
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 7. Vector Dict format
void readHitWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    auto view_HitVector = ntuple->GetView<HitVector>("HitVector");
    for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
        const HitVector& hit = view_HitVector(i);
        const auto& vec = hit.getPeakTime();
        volatile auto* ptr = &vec;
    }
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 8. Individual Dict format
void readHitWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    auto view_HitIndividual = ntuple->GetView<HitIndividual>("HitIndividual");
    for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
        const HitIndividual& hit = view_HitIndividual(i);
        volatile auto* ptr = &hit.fPeakTime;
    }
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 9. Split Vector Dict format
void readSplitHitAndWireDataVectorDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    auto view_HitVector = ntuple->GetView<HitVector>("HitVector");
    for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
        const HitVector& hit = view_HitVector(i);
        for (float val : hit.getSigmaPeakTime()) {
            const auto& vec = hit.getSigmaPeakTime();
            volatile auto* ptr = &vec;
        }
    }
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 10. Split Individual Dict format
void readSplitHitAndWireDataIndividualDict(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    auto view_HitIndividual = ntuple->GetView<HitIndividual>("HitIndividual");
    for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
        const HitIndividual& hit = view_HitIndividual(i);
        volatile auto* ptr = &hit.fStartTick;
    }
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 11. Spil Vector Dict format
void readSpilHitAndWireDataVectorDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    auto view_HitVector = ntuple->GetView<HitVector>("HitVector");
    for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
        const HitVector& hit = view_HitVector(i);
        for (int val : hit.getWireID_Wire()) {
            const auto& vec = hit.getWireID_Wire();
            volatile auto* ptr = &vec;
        }
    }
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 12. Spil Individual Dict format
void readSpilHitAndWireDataIndividualDict(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    auto view_HitIndividual = ntuple->GetView<HitIndividual>("HitIndividual");
    for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
        const HitIndividual& hit = view_HitIndividual(i);
        volatile auto* ptr = &hit.fWireID_Wire;
    }
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 13. Spil Vector Dict format (alternate)
void readSpilHitAndWireDataVectorDictAlt(int numEvents, int numSpils, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    int nThreads = get_nthreads();
    auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.push_back(std::async(std::launch::async, [fileName, chunk]() {
            auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
            auto view = ntuple->GetView<std::vector<float>>("PeakAmplitude");
            for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                const auto& vec = view(i);
                volatile auto* ptr = &vec;
            }
        }));
    }
    for (auto& f : futures) f.get();
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms, total PeakAmplitude count: " << futures.size() << std::endl;
}

// 14. Vector of Individuals format
void readHitWireDataVectorOfIndividuals(int numEvents, int hitsPerEvent, int wiresPerEvent, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    auto entryRange = ntuple->GetEntryRange();
    int nThreads = get_nthreads();
    auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.push_back(std::async(std::launch::async, [fileName, chunk]() {
            auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
            auto hitsView = ntuple->GetView<std::vector<HitIndividual>>("Hits");
            for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                const auto& hits = hitsView(i);
                for (const auto& hit : hits) {
                    const auto& vec = hit.fChannel;
                    volatile auto* ptr = &vec;
                }
            }
        }));
    }
    for (auto& f : futures) f.get();
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
} 


void in() {
    int numEvents = 1000;
    int hitsPerEvent = 100;
    int wiresPerEvent = 100;
    int numSpils = 10;

    std::cout << "Reading HitWire data with Vector format (single HitVector)..." << std::endl;
    readHitWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector.root");
    std::cout << "Reading HitWire data with Individual format..." << std::endl;
    readHitWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual.root");
    std::cout << "Reading Split HitWire data with Vector format..." << std::endl;
    readSplitHitAndWireDataVector(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/split_vector.root");
    std::cout << "Reading Split HitWire data with Individual format..." << std::endl;
    readSplitHitAndWireDataIndividual(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/split_individual.root");
    std::cout << "Reading Spil HitWire data with Vector format..." << std::endl;
    readSpilHitAndWireDataVector(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/spil_vector.root");
    std::cout << "Reading Spil HitWire data with Individual format..." << std::endl;
    readSpilHitAndWireDataIndividual(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/spil_individual.root");
    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Reading HitWire data with Vector format (Dict)..." << std::endl;
    readHitWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_dict.root");
    std::cout << "Reading HitWire data with Individual format (Dict)..." << std::endl;
    readHitWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual_dict.root");
    std::cout << "Reading Split HitWire data with Vector format (Dict)..." << std::endl;
    readSplitHitAndWireDataVectorDict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/split_vector_dict.root");
    std::cout << "Reading Split HitWire data with Individual format (Dict)..." << std::endl;
    readSplitHitAndWireDataIndividualDict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/split_individual_dict.root");
    std::cout << "Reading Spil HitWire data with Vector format (Dict)..." << std::endl;
    readSpilHitAndWireDataVectorDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/spil_vector_dict.root");
    std::cout << "Reading Spil HitWire data with Individual format (Dict)..." << std::endl;
    readSpilHitAndWireDataIndividualDict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/spil_individual_dict.root");
    std::cout << "Reading HitWire data with Vector of Individuals format..." << std::endl;
    readHitWireDataVectorOfIndividuals(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_of_individuals.root");
}
