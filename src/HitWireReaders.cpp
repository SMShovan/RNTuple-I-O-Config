#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleReader.hxx>
#include <TFile.h>
#include <TStopwatch.h>
#include "HitWireWriterHelpers.hpp"
#include "TopBatchRow.hpp"
#include "TopBatchRowSOA.hpp"
#include "UnionRow.hpp"
#include "UnionRowSOA.hpp"
#include "ReaderResult.hpp"
#include <iomanip> // For std::setw
#include <future>
#include <thread>
#include <vector>
#include "Utils.hpp" // For split_range_by_clusters
#include "ProgressiveTablePrinter.hpp"
#include <exception>



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
    return sw.RealTime();
}

double readAOS_event_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<std::vector<HitIndividual>>, fileName, "aos_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<WireIndividual>>, fileName, "aos_wires", "wires", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime();
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
    return sw.RealTime();
}

double readAOS_spill_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<EventAOS>(fileName, "aos_spills", "EventAOS", nThreads);
    sw.Stop();
    return sw.RealTime();
}

double readAOS_spill_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<std::vector<HitIndividual>>, fileName, "aos_spill_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<WireIndividual>>, fileName, "aos_spill_wires", "wires", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime();
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
    return sw.RealTime();
}

double readAOS_topObject_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<HitIndividual>, fileName, "aos_top_hits", "hit", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<WireIndividual>, fileName, "aos_top_wires", "wire", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime();
}

double readAOS_topObject_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<HitIndividual>, fileName, "aos_top_hits", "hit", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<WireBase>, fileName, "aos_top_wires", "wire", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<std::vector<FlatROI>>, fileName, "aos_top_rois", "rois", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime();
}

double readAOS_element_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<HitIndividual>, fileName, "element_hits", "hit", nThreads);
    auto wireROIFuture = std::async(std::launch::async, processNtuple<WireROI>, fileName, "element_wire_rois", "wire_roi", nThreads);
    hitsFuture.get();
    wireROIFuture.get();
    sw.Stop();
    return sw.RealTime();
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
    return sw.RealTime();
}

double readAOS_topObject_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<AOSTopBatchRow>(fileName, "aos_top_all", "row", nThreads);
    sw.Stop();
    return sw.RealTime();
}

double readAOS_element_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<AOSUnionRow>(fileName, "aos_element_all", "row", nThreads);
    sw.Stop();
    return sw.RealTime();
}

// Define missing SOA structs
// struct EventSOA { // Removed
//     HitVector hits; // Removed
//     WireVector wires; // Removed
//     ClassDef(EventSOA, 1); // Removed
// }; // Removed

// struct SOAROI { // Removed
//     unsigned int WireID; // Removed
//     std::size_t offset; // Removed
//     std::vector<float> data; // Removed
//     ClassDef(SOAROI, 1); // Removed
// }; // Removed

// SOA Traverse Overloads
// void traverse(const EventSOA& event) { // Removed
//     for (size_t i = 0; i < event.hits.fChannel.size(); ++i) {  // Use a field like fChannel // Removed
//         volatile float sink = event.hits.fPeakAmplitude[i]; (void)sink; // Removed
//     } // Removed
//     for (size_t w = 0; w < event.wires.fWire_Channel.size(); ++w) { // Removed
//         volatile unsigned int sink = event.wires.fWire_Channel[w]; (void)sink; // Removed
//         auto start = event.wires.fSignalROI_offsets[w]; // Removed
//         auto end = start + event.wires.fSignalROI_nROIs[w] * 10;  // Assume fixed size or adjust // Removed
//         for (auto r = start; r < end; r += 10) { // Removed
//             volatile float sink2 = event.wires.fSignalROI_data[r]; (void)sink2; // Removed
//         } // Removed
//     } // Removed
// } // Removed

// void traverse(const HitVector& hits) { // Removed
//     for (size_t i = 0; i < hits.fChannel.size(); ++i) { // Removed
//         volatile float sink = hits.fPeakAmplitude[i]; (void)sink; // Removed
//     } // Removed
// } // Removed

// void traverse(const WireVector& wires) { // Removed
//     for (size_t w = 0; w < wires.fWire_Channel.size(); ++w) { // Removed
//         volatile unsigned int sink = wires.fWire_Channel[w]; (void)sink; // Removed
//         auto start = wires.fSignalROI_offsets[w]; // Removed
//         auto end = start + wires.fSignalROI_nROIs[w] * 10; // Removed
//         for (auto r = start; r < end; r += 10) { // Removed
//             volatile float sink2 = wires.fSignalROI_data[r]; (void)sink2; // Removed
//         } // Removed
//     } // Removed
// } // Removed

// void traverse(const std::vector<WireBase>& wires) { // Removed
//     for (const auto& w : wires) { // Removed
//         volatile unsigned int sink = w.fWire_Channel; (void)sink; // Removed
//     } // Removed
// } // Removed

// void traverse(const std::vector<SOAROI>& rois) { // Removed
//     for (const auto& r : rois) { // Removed
//         if (!r.data.empty()) { // Removed
//             volatile float sink = r.data[0]; (void)sink; // Removed
//         } // Removed
//     } // Removed
// } // Removed

// SOA Reader Functions
double readSOA_event_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<EventSOA>(fileName, "soa_events", "EventSOA", nThreads);
    sw.Stop();
    return sw.RealTime();
}

double readSOA_event_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHitVector>, fileName, "soa_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<SOAWireVector>, fileName, "soa_wires", "wires", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime();
}

double readSOA_event_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHitVector>, fileName, "soa_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<SOAWireBase>>, fileName, "soa_wires", "wires", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<std::vector<SOAROI>>, fileName, "soa_rois", "rois", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime();
}

// Group 2 readers
double readSOA_spill_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<EventSOA>(fileName, "soa_spill_all", "EventSOA", nThreads);
    sw.Stop();
    return sw.RealTime();
}

// Define missing readers
double readSOA_spill_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHitVector>, fileName, "soa_spill_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<SOAWireVector>, fileName, "soa_spill_wires", "wires", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime();
}

double readSOA_spill_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHitVector>, fileName, "soa_spill_hits", "hits", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<std::vector<SOAWireBase>>, fileName, "soa_spill_wires", "wires", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<std::vector<SOAROI>>, fileName, "soa_spill_rois", "rois", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime();
}

// Traverse already covers EventSOA, SOAHitVector, etc.

// Group 3 readers
double readSOA_topObject_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHit>, fileName, "soa_top_hits", "hit", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<SOAWire>, fileName, "soa_top_wires", "wire", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    sw.Stop();
    return sw.RealTime();
}

// Add readSOA_topObject_perGroup with three futures.

// Add traverse for SOAHit, SOAWire, std::vector<SOAROI>.
void traverse(const SOAHit& hit) {
    volatile float sink = hit.fPeakAmplitude; (void)sink;
}

void traverse(const SOAWire& wire) {
    volatile unsigned int sink = wire.fWire_Channel; (void)sink;
    for (const auto& roi : wire.fSignalROI) {
        if (!roi.data.empty()) {
            volatile float sink2 = roi.data[0]; (void)sink2;
        }
    }
}

void traverse(const SOAWireBase& wire) {
    volatile unsigned int sink = wire.fWire_Channel; (void)sink;
}

// Group 4 readers
double readSOA_element_perDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHit>, fileName, "soa_element_hits", "hit", nThreads);
    // After switching perDataProduct to ROI-per-row, read FlatSOAROI from soa_element_rois
    auto roisFuture = std::async(std::launch::async, processNtuple<FlatSOAROI>, fileName, "soa_element_rois", "roi", nThreads);
    hitsFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime();
}

// Add readSOA_element_perGroup with three futures for hit, wire, roi.

// Add traverse for FlatSOAROI if used
void traverse(const FlatSOAROI& roi) {
    volatile unsigned int sinkID = roi.EventID; (void)sinkID;
    volatile unsigned int sinkWire = roi.WireID; (void)sinkWire;
    if (!roi.data.empty()) {
        volatile float sink = roi.data[0]; (void)sink;
    }
}

void traverse(const SOAROI& roi) {
    if (!roi.data.empty()) {
        volatile float sink = roi.data[0]; (void)sink;
    }
}

void traverse(const std::vector<SOAROI>& rois) {
    for (const auto& r : rois) {
        if (!r.data.empty()) {
            volatile float sink = r.data[0]; (void)sink;
        }
    }
}

void traverse(const SOATopBatchRow& row) {
    if (row.hasHit) traverse(row.hit);
    if (row.hasWire) {
        traverse(row.wire);
        traverse(row.rois);
    }
}

void traverse(const SOAUnionRow& row) {
    switch (row.recordType) {
        case 0: traverse(row.hit); break;
        case 1: traverse(row.wire); break;
        case 2: traverse(row.roi); break;
        default: break;
    }
}

double readSOA_topObject_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHit>, fileName, "soa_top_hits", "hit", nThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<SOAWireBase>, fileName, "soa_top_wires", "wire", nThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<std::vector<SOAROI>>, fileName, "soa_top_rois", "rois", nThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime();
}

double readSOA_element_perGroup(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    int localThreads = std::max(1, nThreads / 3);
    auto hitsFuture = std::async(std::launch::async, processNtuple<SOAHit>, fileName, "soa_element_hits", "hit", localThreads);
    auto wiresFuture = std::async(std::launch::async, processNtuple<SOAWireBase>, fileName, "soa_element_wires", "wire", localThreads);
    auto roisFuture = std::async(std::launch::async, processNtuple<FlatSOAROI>, fileName, "soa_element_rois", "roi", localThreads);
    hitsFuture.get();
    wiresFuture.get();
    roisFuture.get();
    sw.Stop();
    return sw.RealTime();
}

double readSOA_topObject_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<SOATopBatchRow>(fileName, "soa_top_all", "row", nThreads);
    sw.Stop();
    return sw.RealTime();
}

double readSOA_element_allDataProduct(const std::string& fileName, int nThreads) {
    TStopwatch sw;
    sw.Start();
    processNtuple<SOAUnionRow>(fileName, "soa_element_all", "row", nThreads);
    sw.Stop();
    return sw.RealTime();
}

std::vector<ReaderResult> inAOS(int nThreads, int iter, const std::string& outputDir, int mask /*= -1*/) {
    std::vector<ReaderResult> results;
    
    // Create progressive table printer
    ProgressiveTablePrinter<ReaderResult> tablePrinter(
        "AOS Reader Benchmarks (Progressive Results)",
        {"Reader", "Cold (s)", "Warm Avg (s)", "Warm StdDev (s)", "Itr 1 (s)", "Itr 2 (s)", "Itr 3 (s)"},
        {32, 16, 16, 16, 12, 12, 12}
    );
    
    auto benchmark = [&](const std::string& label, auto readerFunc, const std::string& file) {
        ReaderResult result = {label, 0.0, 0.0, 0.0, {}, {}, false, ""};
        
        try {
            std::vector<double> coldTimes, warmTimes;
            if (iter > 0) {
                double cold = readerFunc(file, nThreads);
                coldTimes.push_back(cold);
            }
            for (int i = 1; i < iter; ++i) {
                double warm = readerFunc(file, nThreads);
                warmTimes.push_back(warm);
            }
            result.cold = coldTimes.empty() ? 0.0 : coldTimes.front();
            if (!warmTimes.empty()) {
                double warmAvg = std::accumulate(warmTimes.begin(), warmTimes.end(), 0.0) / static_cast<double>(warmTimes.size());
                double warmSqSum = std::inner_product(warmTimes.begin(), warmTimes.end(), warmTimes.begin(), 0.0);
                double denom = static_cast<double>(warmTimes.size() > 1 ? (warmTimes.size() - 1) : 1);
                double warmStddev = std::sqrt(std::max(0.0, (warmSqSum - warmTimes.size() * warmAvg * warmAvg) / denom));
                result.warmAvg = warmAvg;
                result.warmStddev = warmStddev;
            } else {
                result.warmAvg = 0.0;
                result.warmStddev = 0.0;
            }
            result.coldTimes = coldTimes; // Store individual cold time (first iteration)
            result.warmTimes = warmTimes; // Store individual warm times (remaining iterations)
        } catch (const std::exception& e) {
            std::cout << "Running " << label << "... FAILED" << std::endl;
            result.failed = true;
            result.errorMessage = e.what();
        } catch (...) {
            std::cout << "Running " << label << "... FAILED" << std::endl;
            result.failed = true;
            result.errorMessage = "Unknown error occurred";
        }
        
        results.push_back(result);
        tablePrinter.addRow(result);
    };

    auto shouldRun = [&](int idx) { return mask < 0 || ((mask & (1 << idx)) != 0); };
    if (shouldRun(0))  benchmark("AOS_event_allDataProduct",     readAOS_event_allDataProduct,     outputDir + "/aos_event_all.root");
    if (shouldRun(1))  benchmark("AOS_event_perDataProduct",     readAOS_event_perDataProduct,     outputDir + "/aos_event_perData.root");
    if (shouldRun(2))  benchmark("AOS_event_perGroup",           readAOS_event_perGroup,           outputDir + "/aos_event_perGroup.root");
    if (shouldRun(3))  benchmark("AOS_spill_allDataProduct",     readAOS_spill_allDataProduct,     outputDir + "/aos_spill_all.root");
    if (shouldRun(4))  benchmark("AOS_spill_perDataProduct",     readAOS_spill_perDataProduct,     outputDir + "/aos_spill_perData.root");
    if (shouldRun(5))  benchmark("AOS_spill_perGroup",           readAOS_spill_perGroup,           outputDir + "/aos_spill_perGroup.root");
    if (shouldRun(6))  benchmark("AOS_topObject_allDataProduct", readAOS_topObject_allDataProduct, outputDir + "/aos_topObject_all.root");
    if (shouldRun(7))  benchmark("AOS_topObject_perDataProduct", readAOS_topObject_perDataProduct, outputDir + "/aos_topObject_perData.root");
    if (shouldRun(8))  benchmark("AOS_topObject_perGroup",       readAOS_topObject_perGroup,       outputDir + "/aos_topObject_perGroup.root");
    if (shouldRun(9))  benchmark("AOS_element_allDataProduct",   readAOS_element_allDataProduct,   outputDir + "/aos_element_all.root");
    if (shouldRun(10)) benchmark("AOS_element_perDataProduct",   readAOS_element_perDataProduct,   outputDir + "/aos_element_perData.root");
    if (shouldRun(11)) benchmark("AOS_element_perGroup",         readAOS_element_perGroup,         outputDir + "/aos_element_perGroup.root");

    tablePrinter.printFooter();
    return results;
}

std::vector<ReaderResult> inSOA(int nThreads, int iter, const std::string& outputDir, int mask /*= -1*/) {
    std::vector<ReaderResult> results;
    
    // Create progressive table printer
    ProgressiveTablePrinter<ReaderResult> tablePrinter(
        "SOA Reader Benchmarks (Progressive Results)",
        {"SOA Reader", "Cold (s)", "Warm Avg (s)", "Warm StdDev (s)", "Itr 1 (s)", "Itr 2 (s)", "Itr 3 (s)"},
        {32, 16, 16, 16, 12, 12, 12}
    );
    
    auto benchmark = [&](const std::string& label, auto readerFunc, const std::string& file) {
        ReaderResult result = {label, 0.0, 0.0, 0.0, {}, {}, false, ""};
        
        try {
            std::vector<double> coldTimes, warmTimes;
            if (iter > 0) {
                double cold = readerFunc(file, nThreads);
                coldTimes.push_back(cold);
            }
            for (int i = 1; i < iter; ++i) {
                double warm = readerFunc(file, nThreads);
                warmTimes.push_back(warm);
            }
            result.cold = coldTimes.empty() ? 0.0 : coldTimes.front();
            if (!warmTimes.empty()) {
                double warmAvg = std::accumulate(warmTimes.begin(), warmTimes.end(), 0.0) / static_cast<double>(warmTimes.size());
                double warmSqSum = std::inner_product(warmTimes.begin(), warmTimes.end(), warmTimes.begin(), 0.0);
                double denom = static_cast<double>(warmTimes.size() > 1 ? (warmTimes.size() - 1) : 1);
                double warmStddev = std::sqrt(std::max(0.0, (warmSqSum - warmTimes.size() * warmAvg * warmAvg) / denom));
                result.warmAvg = warmAvg;
                result.warmStddev = warmStddev;
            } else {
                result.warmAvg = 0.0;
                result.warmStddev = 0.0;
            }
            result.coldTimes = coldTimes; // Store individual cold time (first iteration)
            result.warmTimes = warmTimes; // Store individual warm times (remaining iterations)
        } catch (const std::exception& e) {
            std::cout << "Running " << label << "... FAILED" << std::endl;
            result.failed = true;
            result.errorMessage = e.what();
        } catch (...) {
            std::cout << "Running " << label << "... FAILED" << std::endl;
            result.failed = true;
            result.errorMessage = "Unknown error occurred";
        }
        
        results.push_back(result);
        tablePrinter.addRow(result);
    };

    auto shouldRun = [&](int idx) { return mask < 0 || ((mask & (1 << idx)) != 0); };
    if (shouldRun(0))  benchmark("SOA_event_allDataProduct",     readSOA_event_allDataProduct,     outputDir + "/soa_event_all.root");
    if (shouldRun(1))  benchmark("SOA_event_perDataProduct",     readSOA_event_perDataProduct,     outputDir + "/soa_event_perData.root");
    if (shouldRun(2))  benchmark("SOA_event_perGroup",           readSOA_event_perGroup,           outputDir + "/soa_event_perGroup.root");
    if (shouldRun(3))  benchmark("SOA_spill_allDataProduct",     readSOA_spill_allDataProduct,     outputDir + "/soa_spill_all.root");
    if (shouldRun(4))  benchmark("SOA_spill_perDataProduct",     readSOA_spill_perDataProduct,     outputDir + "/soa_spill_perData.root");
    if (shouldRun(5))  benchmark("SOA_spill_perGroup",           readSOA_spill_perGroup,           outputDir + "/soa_spill_perGroup.root");
    if (shouldRun(6))  benchmark("SOA_topObject_allDataProduct", readSOA_topObject_allDataProduct, outputDir + "/soa_topObject_all.root");
    if (shouldRun(7))  benchmark("SOA_topObject_perDataProduct", readSOA_topObject_perDataProduct, outputDir + "/soa_topObject_perData.root");
    if (shouldRun(8))  benchmark("SOA_topObject_perGroup",       readSOA_topObject_perGroup,       outputDir + "/soa_topObject_perGroup.root");
    if (shouldRun(9))  benchmark("SOA_element_allDataProduct",   readSOA_element_allDataProduct,   outputDir + "/soa_element_all.root");
    if (shouldRun(10)) benchmark("SOA_element_perDataProduct",   readSOA_element_perDataProduct,   outputDir + "/soa_element_perData.root");
    if (shouldRun(11)) benchmark("SOA_element_perGroup",         readSOA_element_perGroup,         outputDir + "/soa_element_perGroup.root");

    tablePrinter.printFooter();
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
    volatile unsigned int sinkEvt  = roi.EventID; (void)sinkEvt;
    volatile unsigned int sinkWire = roi.WireID; (void)sinkWire;
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

void traverse(const AOSTopBatchRow& row) {
    if (row.hasHit) traverse(row.hit);
    if (row.hasWire) {
        traverse(row.wire);
        traverse(row.rois);
    }
}

void traverse(const AOSUnionRow& row) {
    switch (row.recordType) {
        case 0: traverse(row.hit); break;
        case 1: traverse(row.wire); break;
        case 2: traverse(row.roi); break;
        default: break;
    }
}

void traverse(const EventSOA& event) {
    for (size_t i = 0; i < event.hits.EventIDs.size(); ++i) {
        volatile float sink = event.hits.fPeakAmplitude[i]; (void)sink;
    }
    for (size_t w = 0; w < event.wires.EventIDs.size(); ++w) {
        volatile unsigned int sink = event.wires.fWire_Channel[w]; (void)sink;
        for (const auto& roi : event.wires.fSignalROI[w]) {
            if (!roi.data.empty()) {
                volatile float sink2 = roi.data[0]; (void)sink2;
            }
        }
    }
}

void traverse(const SOAHitVector& hits) {
    for (size_t i = 0; i < hits.EventIDs.size(); ++i) {
        volatile float sink = hits.fPeakAmplitude[i]; (void)sink;
    }
}

void traverse(const SOAWireVector& wires) {
    for (size_t w = 0; w < wires.EventIDs.size(); ++w) {
        volatile unsigned int sink = wires.fWire_Channel[w]; (void)sink;
        for (const auto& roi : wires.fSignalROI[w]) {
            if (!roi.data.empty()) {
                volatile float sink2 = roi.data[0]; (void)sink2;
            }
        }
    }
}

void traverse(const std::vector<SOAWireBase>& wires) {
    for (const auto& w : wires) {
        volatile unsigned int sink = w.fWire_Channel; (void)sink;
    }
}