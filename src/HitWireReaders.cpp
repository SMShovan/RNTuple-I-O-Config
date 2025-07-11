#include <ROOT/RNTupleReader.hxx>
#include <TStopwatch.h>
#include <future>
#include <thread>
#include <iostream>
#include <vector>
#include "Hit.hpp"
#include "Wire.hpp"

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
void read_Hit_Wire_Vector(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();

    // Parallel read for both "hits" and "wires" ntuples
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) {
            std::cerr << "Could not open ntuple " << ntupleName << " in " << fileName << std::endl;
            return;
        }
        auto entryRange = pilot->GetEntryRange();
        int nThreads = get_nthreads();
        auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);

        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<HitVector>("HitVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hit = view(i);
                        volatile const auto* sink = &hit.getChannel();
                        (void)sink;
                    }
                } else { // wires
                    auto view = ntuple->GetView<WireVector>("WireVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* sink = &wire.getWire_Channel();
                        (void)sink;
                    }
                }
            }));
        }
        for (auto& f : futures) f.get();
    };

    std::future<void> hitsFuture = std::async(std::launch::async, processNtuple, "hits");
    std::future<void> wiresFuture = std::async(std::launch::async, processNtuple, "wires");

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 2. Split Vector format
void read_VertiSplit_Hit_Wire_Vector(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer; timer.Start();

    auto processNtuple = [&](const std::string& ntupleName){
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto range = pilot->GetEntryRange();
        int nThreads = get_nthreads();
        auto chunks = split_range(*range.begin(), *range.end(), nThreads);
        std::vector<std::future<void>> futs;
        for(const auto &chunk: chunks){
            futs.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk](){
                auto nt = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto startTick = nt->GetView<std::vector<int>>("StartTick");
                    for(std::size_t i=chunk.first;i<chunk.second;++i){
                        const auto &v = startTick(i);
                        volatile auto* sink=&v; (void)sink;
                    }
                } else {
                    auto view = nt->GetView<WireVector>("WireVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* sink = &wire.getWire_Channel();
                        (void)sink;
                    }
                }
            }));
        }
        for(auto &f: futs) f.get();
    };
    
    std::future<void> hitsFuture = std::async(std::launch::async, processNtuple, "hits");
    std::future<void> wiresFuture = std::async(std::launch::async, processNtuple, "wires");

    hitsFuture.get();
    wiresFuture.get();
    
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime()*1000 << " ms" << std::endl;
}

// 3. Spil Vector format
void read_HoriSpill_Hit_Wire_Vector(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto entryRange = pilot->GetEntryRange();
        int nThreads = get_nthreads();
        auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.push_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto spilID = ntuple->GetView<int>("SpilID");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& val = spilID(i);
                        volatile auto* ptr = &val;
                        (void)ptr;
                    }
                } else {
                    auto view = ntuple->GetView<WireVector>("WireVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* sink = &wire.getWire_Channel();
                        (void)sink;
                    }
                }
            }));
        }
        for (auto& f : futures) f.get();
    };
    
    std::future<void> hitsFuture = std::async(std::launch::async, processNtuple, "hits");
    std::future<void> wiresFuture = std::async(std::launch::async, processNtuple, "wires");

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 4. Individual format
void read_Hit_Wire_Individual(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto entryRange = pilot->GetEntryRange();
        int nThreads = get_nthreads();
        auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.push_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto channel = ntuple->GetView<unsigned int>("Channel");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& val = channel(i);
                        volatile auto* ptr = &val;
                        (void)ptr;
                    }
                } else {
                    auto view = ntuple->GetView<WireIndividual>("WireIndividual");
                     for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* sink = &wire.fWire_Channel;
                        (void)sink;
                    }
                }
            }));
        }
        for (auto& f : futures) f.get();
    };
    
    std::future<void> hitsFuture = std::async(std::launch::async, processNtuple, "hits");
    std::future<void> wiresFuture = std::async(std::launch::async, processNtuple, "wires");

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 5. Split Individual format
void read_VertiSplit_Hit_Wire_Individual(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto entryRange = pilot->GetEntryRange();
        int nThreads = get_nthreads();
        auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.push_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto startTick = ntuple->GetView<int>("StartTick");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& val = startTick(i);
                        volatile auto* ptr = &val;
                        (void)ptr;
                    }
                } else {
                     auto view = ntuple->GetView<WireIndividual>("WireIndividual");
                     for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* sink = &wire.fWire_Channel;
                        (void)sink;
                    }
                }
            }));
        }
        for (auto& f : futures) f.get();
    };

    std::future<void> hitsFuture = std::async(std::launch::async, processNtuple, "hits");
    std::future<void> wiresFuture = std::async(std::launch::async, processNtuple, "wires");

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 6. Spil Individual format
void read_HoriSpill_Hit_Wire_Data_Individual(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
     auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto entryRange = pilot->GetEntryRange();
        int nThreads = get_nthreads();
        auto chunks = split_range(*entryRange.begin(), *entryRange.end(), nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.push_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto spilID = ntuple->GetView<int>("SpilID");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& val = spilID(i);
                        volatile auto* ptr = &val;
                        (void)ptr;
                    }
                } else {
                    auto view = ntuple->GetView<WireIndividual>("WireIndividual");
                     for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* sink = &wire.fWire_Channel;
                        (void)sink;
                    }
                }
            }));
        }
        for (auto& f : futures) f.get();
    };
    
    std::future<void> hitsFuture = std::async(std::launch::async, processNtuple, "hits");
    std::future<void> wiresFuture = std::async(std::launch::async, processNtuple, "wires");

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 7. Vector Dict format
void read_Hit_Wire_Vector_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();

    auto readHits = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<HitVector>("HitVector");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& hit = view(i);
            volatile const auto* sink = &hit.getPeakTime();
            (void)sink;
        }
    };

    auto readWires = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("wires", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<WireVector>("WireVector");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& wire = view(i);
            volatile const auto* sink = &wire.getWire_Channel();
            (void)sink;
        }
    };

    std::future<void> hitsFuture = std::async(std::launch::async, readHits);
    std::future<void> wiresFuture = std::async(std::launch::async, readWires);

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 8. Individual Dict format
void read_Hit_Wire_Individual_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto readHits = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<HitIndividual>("HitIndividual");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& hit = view(i);
            volatile auto* ptr = &hit.fPeakTime;
            (void)ptr;
        }
    };

    auto readWires = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("wires", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<WireIndividual>("WireIndividual");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& wire = view(i);
            volatile auto* ptr = &wire.fWire_Channel;
            (void)ptr;
        }
    };
    
    std::future<void> hitsFuture = std::async(std::launch::async, readHits);
    std::future<void> wiresFuture = std::async(std::launch::async, readWires);

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 9. Split Vector Dict format
void read_VertiSplit_Hit_Wire_Vector_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();

    auto readHits = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<HitVector>("HitVector");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& hit = view(i);
            volatile const auto* ptr = &hit.getSigmaPeakTime();
            (void)ptr;
        }
    };

    auto readWires = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("wires", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<WireVector>("WireVector");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& wire = view(i);
            volatile const auto* ptr = &wire.getWire_Channel();
            (void)ptr;
        }
    };

    std::future<void> hitsFuture = std::async(std::launch::async, readHits);
    std::future<void> wiresFuture = std::async(std::launch::async, readWires);

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 10. Split Individual Dict format
void read_VertiSplit_Hit_Wire_Individual_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto readHits = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<HitIndividual>("HitIndividual");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& hit = view(i);
            volatile auto* ptr = &hit.fStartTick;
            (void)ptr;
        }
    };

    auto readWires = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("wires", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<WireIndividual>("WireIndividual");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& wire = view(i);
            volatile auto* ptr = &wire.fWire_Channel;
            (void)ptr;
        }
    };

    std::future<void> hitsFuture = std::async(std::launch::async, readHits);
    std::future<void> wiresFuture = std::async(std::launch::async, readWires);

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 11. Spil Vector Dict format
void read_HoriSpill_Hit_Wire_Vector_Dict(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto readHits = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<HitVector>("HitVector");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& hit = view(i);
            volatile const auto* ptr = &hit.EventID;
            (void)ptr;
        }
    };
    auto readWires = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("wires", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<WireVector>("WireVector");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& wire = view(i);
            volatile const auto* ptr = &wire.EventID;
            (void)ptr;
        }
    };
    std::future<void> hitsFuture = std::async(std::launch::async, readHits);
    std::future<void> wiresFuture = std::async(std::launch::async, readWires);

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 12. Spil Individual Dict format
void read_HoriSpill_Hit_Wire_Individual_Dict(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();

    auto readHits = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<HitIndividual>("HitIndividual");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& hit = view(i);
            volatile const auto* ptr = &hit.EventID;
            (void)ptr;
        }
    };
    
    auto readWires = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("wires", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<WireIndividual>("WireIndividual");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& wire = view(i);
            volatile const auto* ptr = &wire.EventID;
            (void)ptr;
        }
    };
    
    std::future<void> hitsFuture = std::async(std::launch::async, readHits);
    std::future<void> wiresFuture = std::async(std::launch::async, readWires);

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

void readSpilHitAndWireDataVectorDictAlt(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();

    auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
    if (!ntuple) return;
    auto view_HitVector = ntuple->GetView<HitVector>("HitVector");
    for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
        const HitVector& hit = view_HitVector(i);
        volatile const auto* ptr = &hit.EventID;
        (void)ptr;
    }
    
    auto ntuple_wires = ROOT::RNTupleReader::Open("wires", fileName);
    if (!ntuple_wires) return;
    auto view_WireVector = ntuple_wires->GetView<WireVector>("WireVector");
    for (std::size_t i = 0; i < ntuple_wires->GetNEntries(); ++i) {
        const WireVector& wire = view_WireVector(i);
        volatile const auto* ptr = &wire.EventID;
        (void)ptr;
    }
    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}

// 13. Vector of Individuals format
void read_Hit_Wire_Vector_Of_Individuals(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
    TStopwatch timer;
    timer.Start();
    auto readHits = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<std::vector<HitIndividual>>("Hits");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& hits = view(i);
            volatile const auto* sink = &hits;
            (void)sink;
        }
    };

    auto readWires = [&]() {
        auto ntuple = ROOT::RNTupleReader::Open("wires", fileName);
        if (!ntuple) return;
        auto view = ntuple->GetView<std::vector<WireIndividual>>("Wires");
        for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
            const auto& wires = view(i);
            volatile const auto* sink = &wires;
            (void)sink;
        }
    };
    
    std::future<void> hitsFuture = std::async(std::launch::async, readHits);
    std::future<void> wiresFuture = std::async(std::launch::async, readWires);

    hitsFuture.get();
    wiresFuture.get();

    timer.Stop();
    std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
}


void in() {
    int numEvents = 1000;
    int hitsPerEvent = 100;
    int wiresPerEvent = 100;
    int numSpils = 10;

    std::cout << "Reading HitWire data with Vector format (single HitVector)..." << std::endl;
    read_Hit_Wire_Vector(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector.root");
    std::cout << "Reading HitWire data with Individual format..." << std::endl;
    read_Hit_Wire_Individual(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual.root");
    std::cout << "Reading VertiSplit HitWire data with Vector format..." << std::endl;
    read_VertiSplit_Hit_Wire_Vector(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_vector.root");
    std::cout << "Reading VertiSplit HitWire data with Individual format..." << std::endl;
    read_VertiSplit_Hit_Wire_Individual(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_individual.root");
    std::cout << "Reading HoriSpill HitWire data with Vector format..." << std::endl;
    read_HoriSpill_Hit_Wire_Vector(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_vector.root");
    std::cout << "Reading HoriSpill HitWire data with Individual format..." << std::endl;
    read_HoriSpill_Hit_Wire_Data_Individual(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_individual.root");
    std::cout << "\n--- DICTIONARY-BASED EXPERIMENTS ---" << std::endl;
    std::cout << "Reading HitWire data with Vector format (Dict)..." << std::endl;
    read_Hit_Wire_Vector_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_dict.root");
    std::cout << "Reading HitWire data with Individual format (Dict)..." << std::endl;
    read_Hit_Wire_Individual_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual_dict.root");
    std::cout << "Reading VertiSplit HitWire data with Vector format (Dict)..." << std::endl;
    read_VertiSplit_Hit_Wire_Vector_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_vector_dict.root");
    std::cout << "Reading VertiSplit HitWire data with Individual format (Dict)..." << std::endl;
    read_VertiSplit_Hit_Wire_Individual_Dict(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_individual_dict.root");
    std::cout << "Reading HoriSpill HitWire data with Vector format (Dict)..." << std::endl;
    read_HoriSpill_Hit_Wire_Vector_Dict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_vector_dict.root");
    std::cout << "Reading HoriSpill HitWire data with Individual format (Dict)..." << std::endl;
    read_HoriSpill_Hit_Wire_Individual_Dict(numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_individual_dict.root");
    std::cout << "Reading HitWire data with Vector of Individuals format..." << std::endl;
    read_Hit_Wire_Vector_Of_Individuals(numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_of_individuals.root");
}
