#include <ROOT/RNTupleReader.hxx>
#include <TStopwatch.h>
#include <future>
#include <iostream>
#include <vector>
#include <numeric>
#include <cmath>
#include "Hit.hpp"
#include "Wire.hpp"
#include "Utils.hpp"
#include <iomanip> // For table formatting

#include "ReaderResult.hpp"

using namespace Utils;


// Match writers: central output folder
static const std::string kOutputDir = "./output";


// 1. Vector format (single HitVector column)
double read_Hit_Wire_Vector(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();

    // Parallel read for both "hits" and "wires" ntuples
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) {
            std::cerr << "Could not open ntuple " << ntupleName << " in " << fileName << std::endl;
            return;
        }
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);

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
    return timer.RealTime() * 1000;
}

// 2. Split Vector format
double read_VertiSplit_Hit_Wire_Vector(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer; timer.Start();

    auto processNtuple = [&](const std::string& ntupleName){
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
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
    return timer.RealTime()*1000;
}

// 3. Spil Vector format
double read_HoriSpill_Hit_Wire_Vector(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
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
    return timer.RealTime() * 1000;
}

// 4. Individual format
double read_Hit_Wire_Individual(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
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
    return timer.RealTime() * 1000;
}

// 5. Split Individual format
double read_VertiSplit_Hit_Wire_Individual(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
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
    return timer.RealTime() * 1000;
}

// 6. Spil Individual format
double read_HoriSpill_Hit_Wire_Data_Individual(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
     auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
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
    return timer.RealTime() * 1000;
}

// 7. Vector Dict format
double read_Hit_Wire_Vector_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();

    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<HitVector>("HitVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hit = view(i);
                        volatile const auto* sink = &hit.getPeakTime();
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
    return timer.RealTime() * 1000;
}

// 8. Individual Dict format
double read_Hit_Wire_Individual_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<HitIndividual>("HitIndividual");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hit = view(i);
                        volatile auto* ptr = &hit.fPeakTime;
                        (void)ptr;
                    }
                } else { // wires
                    auto view = ntuple->GetView<WireIndividual>("WireIndividual");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile auto* ptr = &wire.fWire_Channel;
                        (void)ptr;
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
    return timer.RealTime() * 1000;
}

// 9. Split Vector Dict format
double read_VertiSplit_Hit_Wire_Vector_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();

    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<HitVector>("HitVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hit = view(i);
                        volatile const auto* ptr = &hit.getSigmaPeakTime();
                        (void)ptr;
                    }
                } else { // wires
                    auto view = ntuple->GetView<WireVector>("WireVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* ptr = &wire.getWire_Channel();
                        (void)ptr;
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
    return timer.RealTime() * 1000;
}

// 10. Split Individual Dict format
double read_VertiSplit_Hit_Wire_Individual_Dict(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<HitIndividual>("HitIndividual");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hit = view(i);
                        volatile auto* ptr = &hit.fStartTick;
                        (void)ptr;
                    }
                } else { // wires
                    auto view = ntuple->GetView<WireIndividual>("WireIndividual");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile auto* ptr = &wire.fWire_Channel;
                        (void)ptr;
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
    return timer.RealTime() * 1000;
}

// 11. Spil Vector Dict format
double read_HoriSpill_Hit_Wire_Vector_Dict(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<HitVector>("HitVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hit = view(i);
                        volatile const auto* ptr = &hit.EventID;
                        (void)ptr;
                    }
                } else { // wires
                    auto view = ntuple->GetView<WireVector>("WireVector");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* ptr = &wire.EventID;
                        (void)ptr;
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
    return timer.RealTime() * 1000;
}

// 12. Spil Individual Dict format
double read_HoriSpill_Hit_Wire_Individual_Dict(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();

    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<HitIndividual>("HitIndividual");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hit = view(i);
                        volatile const auto* ptr = &hit.EventID;
                        (void)ptr;
                    }
                } else { // wires
                    auto view = ntuple->GetView<WireIndividual>("WireIndividual");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wire = view(i);
                        volatile const auto* ptr = &wire.EventID;
                        (void)ptr;
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
    return timer.RealTime() * 1000;
}

// void readSpilHitAndWireDataVectorDictAlt(int /*numEvents*/, int /*numSpils*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName) {
//     TStopwatch timer;
//     timer.Start();

//     auto ntuple = ROOT::RNTupleReader::Open("hits", fileName);
//     if (!ntuple) return;
//     auto view_HitVector = ntuple->GetView<HitVector>("HitVector");
//     for (std::size_t i = 0; i < ntuple->GetNEntries(); ++i) {
//         const HitVector& hit = view_HitVector(i);
//         volatile const auto* ptr = &hit.EventID;
//         (void)ptr;
//     }
    
//     auto ntuple_wires = ROOT::RNTupleReader::Open("wires", fileName);
//     if (!ntuple_wires) return;
//     auto view_WireVector = ntuple_wires->GetView<WireVector>("WireVector");
//     for (std::size_t i = 0; i < ntuple_wires->GetNEntries(); ++i) {
//         const WireVector& wire = view_WireVector(i);
//         volatile const auto* ptr = &wire.EventID;
//         (void)ptr;
//     }
//     timer.Stop();
//     std::cout << "  RNTuple Read Time: " << timer.RealTime() * 1000 << " ms" << std::endl;
// }

// 13. Vector of Individuals format
double read_Hit_Wire_Vector_Of_Individuals(int /*numEvents*/, int /*hitsPerEvent*/, int /*wiresPerEvent*/, const std::string& fileName, int nThreads) {
    TStopwatch timer;
    timer.Start();
    auto processNtuple = [&](const std::string& ntupleName) {
        auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
        if (!pilot) return;
        auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
        std::vector<std::future<void>> futures;
        for (const auto& chunk : chunks) {
            futures.emplace_back(std::async(std::launch::async, [fileName, ntupleName, chunk]() {
                auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
                if (ntupleName == "hits") {
                    auto view = ntuple->GetView<std::vector<HitIndividual>>("Hits");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& hits = view(i);
                        volatile const auto* sink = &hits;
                        (void)sink;
                    }
                } else { // wires
                    auto view = ntuple->GetView<std::vector<WireIndividual>>("Wires");
                    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
                        const auto& wires = view(i);
                        volatile const auto* sink = &wires;
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
    return timer.RealTime() * 1000;
}


std::vector<ReaderResult> in(int nThreads, int iter) {
    int numEvents = 1000000;
    int hitsPerEvent = 100;
    int wiresPerEvent = 1000;
    int numSpils = 10;
    int numRuns = iter;

    std::vector<ReaderResult> results;

    auto benchmark = [&](const std::string& label, auto readerFunc, auto&&... args) {
        std::vector<double> times;
        for (int i = 0; i < numRuns; ++i) {
            double t = readerFunc(args...);
            times.push_back(t);
        }
        double cold = times[0];
        double warmAvg = 0.0, warmStddev = 0.0;
        if (numRuns > 1) {
            warmAvg = std::accumulate(times.begin() + 1, times.end(), 0.0) / (times.size() - 1);
            double sq_sum = std::inner_product(times.begin() + 1, times.end(), times.begin() + 1, 0.0);
            warmStddev = std::sqrt(sq_sum / (times.size() - 1) - warmAvg * warmAvg);
        }
        results.push_back({label, cold, warmAvg, warmStddev});
    };

    benchmark("Hit/Wire Vector", read_Hit_Wire_Vector, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector.root", nThreads);
    benchmark("Hit/Wire Individual", read_Hit_Wire_Individual, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual.root", nThreads);
    benchmark("VertiSplit-Vector", read_VertiSplit_Hit_Wire_Vector, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_vector.root", nThreads);
    benchmark("VertiSplit-Individual", read_VertiSplit_Hit_Wire_Individual, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_individual.root", nThreads);
    benchmark("HoriSpill-Vector", read_HoriSpill_Hit_Wire_Vector, numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_vector.root", nThreads);
    benchmark("HoriSpill-Individual", read_HoriSpill_Hit_Wire_Data_Individual, numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_individual.root", nThreads);
    benchmark("Vector-Dict", read_Hit_Wire_Vector_Dict, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_dict.root", nThreads);
    benchmark("Individual-Dict", read_Hit_Wire_Individual_Dict, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/individual_dict.root", nThreads);
    benchmark("VertiSplit-Vector-Dict", read_VertiSplit_Hit_Wire_Vector_Dict, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_vector_dict.root", nThreads);
    benchmark("VertiSplit-Individual-Dict", read_VertiSplit_Hit_Wire_Individual_Dict, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/VertiSplit_individual_dict.root", nThreads);
    benchmark("HoriSpill-Vector-Dict", read_HoriSpill_Hit_Wire_Vector_Dict, numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_vector_dict.root", nThreads);
    benchmark("HoriSpill-Individual-Dict", read_HoriSpill_Hit_Wire_Individual_Dict, numEvents, numSpils, hitsPerEvent, wiresPerEvent, kOutputDir + "/HoriSpill_individual_dict.root", nThreads);
    benchmark("Vector-of-Individuals", read_Hit_Wire_Vector_Of_Individuals, numEvents, hitsPerEvent, wiresPerEvent, kOutputDir + "/vector_of_individuals.root", nThreads);

    // Print table
    std::cout << std::left
              << std::setw(32) << "Reader"
              << std::setw(16) << "Cold (ms)"
              << std::setw(20) << "Warm Avg (ms)"
              << std::setw(20) << "Warm StdDev (ms)" << std::endl;
    std::cout << std::string(88, '-') << std::endl;
    for (const auto& r : results) {
        std::cout << std::left
                  << std::setw(32) << r.label
                  << std::setw(16) << r.cold;
        if (numRuns > 1)
            std::cout << std::setw(20) << r.warmAvg << std::setw(20) << r.warmStddev;
        else
            std::cout << std::setw(20) << "-" << std::setw(20) << "-";
        std::cout << std::endl;
    }
    std::cout << std::string(88, '-') << std::endl;

    return results;
}
