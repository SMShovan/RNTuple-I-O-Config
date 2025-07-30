#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleReader.hxx>
#include <TFile.h>
#include <TStopwatch.h>
#include "updatedHitWireWriterHelpers.hpp"
#include "ReaderResult.hpp"
#include <iomanip> // For std::setw
#include <future>
#include <thread>
#include <vector>
#include "Utils.hpp" // For split_range_by_clusters

const std::string kOutputDir = "./output";

template <typename ViewType>
void processNtupleRange(const std::string& fileName, const std::string& ntupleName, const std::string& fieldName, const std::pair<std::size_t, std::size_t>& chunk) {
    auto ntuple = ROOT::RNTupleReader::Open(ntupleName, fileName);
    auto view = ntuple->GetView<ViewType>(fieldName);
    for (std::size_t i = chunk.first; i < chunk.second; ++i) {
        const auto& val = view(i);
        traverse(val);
    }
}

template <typename ViewType>
double processNtuple(const std::string& fileName, const std::string& ntupleName, const std::string& fieldName, int nThreads) {
    auto pilot = ROOT::RNTupleReader::Open(ntupleName, fileName);
    auto chunks = Utils::split_range_by_clusters(*pilot, nThreads);
    std::vector<std::future<void>> futures;
    for (const auto& chunk : chunks) {
        futures.emplace_back(std::async(std::launch::async, processNtupleRange<ViewType>, fileName, ntupleName, fieldName, chunk));
    }
    for (auto& f : futures) f.get();
    return 0.0; // Placeholder, actual time measured outside
}

double readAOS_event_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<EventAOS>(fileName, "aos_events", "EventAOS", nThreads);
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_event_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<std::vector<HitIndividual>>, fileName, "aos_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<WireIndividual>>, fileName, "aos_wires", "wires", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_event_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<std::vector<HitIndividual>>, fileName, "aos_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<WireBase>>, fileName, "aos_wires", "wires", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<std::vector<FlatROI>>, fileName, "aos_rois", "rois", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_spill_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<EventAOS>(fileName, "aos_spills", "EventAOS", nThreads);
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_spill_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<std::vector<HitIndividual>>, fileName, "aos_spill_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<WireIndividual>>, fileName, "aos_spill_wires", "wires", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_spill_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<std::vector<HitIndividual>>, fileName, "aos_spill_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<WireBase>>, fileName, "aos_spill_wires", "wires", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<std::vector<FlatROI>>, fileName, "aos_spill_rois", "rois", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_topObject_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<HitIndividual>, fileName, "top_hits", "hit", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<WireIndividual>, fileName, "top_wires", "wire", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_topObject_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<HitIndividual>, fileName, "top_hits", "hit", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<WireBase>, fileName, "top_wires", "wire", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<std::vector<FlatROI>>, fileName, "top_rois", "rois", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_element_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<HitIndividual>, fileName, "element_hits", "hit", nThreads);
    auto wireROIFuture = std::async(std::launch::async, processNtuple<WireROI>, fileName, "element_wire_rois", "wire_roi", nThreads);
    hitsFuture.get();
    wireROIFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

double readAOS_element_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<HitIndividual>, fileName, "element_hits", "hit", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<WireBase>, fileName, "element_wires", "wire", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<FlatROI>, fileName, "element_rois", "roi", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime() * 1000;
}

std::vector<ReaderResult> updatedIn(int nThreads, int iter) {
    std::vector<ReaderResult> results;
    auto benchmark = [&](const std::string& label, auto readerFunc, const std::string& file) {
        std::vector<double> coldTimes, warmTimes;
        for (int i = 0; i < iter; ++i) {
            double cold = readerFunc(file, nThreads);
            coldTimes.push_back(cold);
            double warm = readerFunc(file, nThreads); // Second read for warm
            warmTimes.push_back(warm);
        }
        double coldAvg = std::accumulate(coldTimes.begin(), coldTimes.end(), 0.0) / iter;
        double warmAvg = std::accumulate(warmTimes.begin(), warmTimes.end(), 0.0) / iter;
        double warmSqSum = std::inner_product(warmTimes.begin(), warmTimes.end(), warmTimes.begin(), 0.0);
        double warmStddev = std::sqrt((warmSqSum - iter * warmAvg * warmAvg) / (iter - 1));
        results.push_back({label, coldAvg, warmAvg, warmStddev});
    };

    benchmark("AOS_event_allDataProduct", readAOS_event_allDataProduct, kOutputDir + "/aos_event_all.root");
    benchmark("AOS_event_perDataProduct", readAOS_event_perDataProduct, kOutputDir + "/aos_event_perData.root");
    benchmark("AOS_event_perGroup", readAOS_event_perGroup, kOutputDir + "/aos_event_perGroup.root");
    benchmark("AOS_spill_allDataProduct", readAOS_spill_allDataProduct, kOutputDir + "/aos_spill_all.root");
    benchmark("AOS_spill_perDataProduct", readAOS_spill_perDataProduct, kOutputDir + "/aos_spill_perData.root");
    benchmark("AOS_spill_perGroup", readAOS_spill_perGroup, kOutputDir + "/aos_spill_perGroup.root");
    benchmark("AOS_topObject_perDataProduct", readAOS_topObject_perDataProduct, kOutputDir + "/aos_topObject_perData.root");
    benchmark("AOS_topObject_perGroup", readAOS_topObject_perGroup, kOutputDir + "/aos_topObject_perGroup.root");
    benchmark("AOS_element_perDataProduct", readAOS_element_perDataProduct, kOutputDir + "/aos_element_perData.root");
    benchmark("AOS_element_perGroup", readAOS_element_perGroup, kOutputDir + "/aos_element_perGroup.root");

    // Print table
    std::cout << std::left << std::setw(32) << "Reader" << std::setw(16) << "Cold (ms)" << std::setw(16) << "Warm Avg (ms)" << std::setw(16) << "Warm StdDev (ms)" << std::endl;
    for (const auto& res : results) {
        std::cout << std::left << std::setw(32) << res.label << std::setw(16) << res.cold << std::setw(16) << res.warmAvg << std::setw(16) << res.warmStddev << std::endl;
    }
    return results;
}

void traverse(const EventAOS& event) {
    for (const auto& h : event.hits) {
        volatile float sink = h.fPeakAmplitude; (void)sink;
    }
    for (const auto& w : event.wires) {
        volatile unsigned int sink = w.fWire_Channel; (void)sink;
        for (const auto& r : w.fSignalROI) {
            if (!r.data.empty()) {
                volatile float sink2 = r.data[0]; (void)sink2;
            }
        }
    }
}

void traverse(const std::vector<HitIndividual>& hits) {
    for (const auto& h : hits) {
        volatile float sink = h.fPeakAmplitude; (void)sink;
    }
}

void traverse(const std::vector<WireIndividual>& wires) {
    for (const auto& w : wires) {
        volatile unsigned int sink = w.fWire_Channel; (void)sink;
        for (const auto& r : w.fSignalROI) {
            if (!r.data.empty()) {
                volatile float sink2 = r.data[0]; (void)sink2;
            }
        }
    }
}

void traverse(const std::vector<WireBase>& wires) {
    for (const auto& w : wires) {
        volatile unsigned int sink = w.fWire_Channel; (void)sink;
    }
}

void traverse(const std::vector<FlatROI>& rois) {
    for (const auto& r : rois) {
        if (!r.data.empty()) {
            volatile float sink = r.data[0]; (void)sink;
        }
    }
}

void traverse(const HitIndividual& hit) {
    volatile float sink = hit.fPeakAmplitude; (void)sink;
}

void traverse(const WireIndividual& wire) {
    volatile unsigned int sink = wire.fWire_Channel; (void)sink;
    for (const auto& r : wire.fSignalROI) {
        if (!r.data.empty()) {
            volatile float sink2 = r.data[0]; (void)sink2;
        }
    }
}

void traverse(const WireBase& wire) {
    volatile unsigned int sink = wire.fWire_Channel; (void)sink;
}

void traverse(const FlatROI& roi) {
    if (!roi.data.empty()) {
        volatile float sink = roi.data[0]; (void)sink;
    }
}

void traverse(const WireROI& wireRoi) {
    volatile unsigned int sink = wireRoi.fWire_Channel; (void)sink;
    if (!wireRoi.roi.data.empty()) {
        volatile float sink2 = wireRoi.roi.data[0]; (void)sink2;
    }
}