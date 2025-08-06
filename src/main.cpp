#include "visualization.hpp"
#include "HitWireReaders.hpp"
#include <TSystem.h>
#include <TROOT.h>
#include <thread>

#include "ReaderResult.hpp"

#include "WriterResult.hpp"

#include <iostream>
#include <filesystem>

#include "HitWireWriters.hpp"
#include <TFile.h>



int main() {
    ROOT::EnableThreadSafety();
    int nThreads = std::thread::hardware_concurrency();
    ROOT::EnableImplicitMT(nThreads);
    gSystem->Load("libWireDict");
    

    auto aos_writer_results = updatedOutAOS(nThreads, 3);
    
    visualize_aos_writer_results(aos_writer_results);
    
    auto aos_reader_results = updatedInAOS(nThreads, 3);
    
    visualize_aos_reader_results(aos_reader_results);

    // Collect AOS file sizes
    std::vector<std::pair<std::string, double>> aos_file_sizes;
    std::vector<std::string> aos_files = {
        "./output/aos_event_all.root",
        "./output/aos_event_perData.root",
        "./output/aos_event_perGroup.root",
        "./output/aos_spill_all.root",
        "./output/aos_spill_perData.root",
        "./output/aos_spill_perGroup.root",
        "./output/aos_topObject_perData.root",
        "./output/aos_topObject_perGroup.root",
        "./output/aos_element_perData.root",
        "./output/aos_element_perGroup.root"
    };
    for (const auto& f : aos_files) {
        auto tfile = TFile::Open(f.c_str(), "READ");
        if (tfile) {
            double sizeMB = tfile->GetSize() / (1024.0 * 1024.0);
            aos_file_sizes.emplace_back(f, sizeMB);
            tfile->Close();
        }
    }
    visualize_aos_file_sizes(aos_file_sizes);

    // Add SOA visualization - use separate SOA-only functions
    auto soa_writer_results = updatedOutSOA(nThreads, 3);
    visualize_soa_writer_results(soa_writer_results);
    auto soa_reader_results = updatedInSOA(nThreads, 3);
    visualize_soa_reader_results(soa_reader_results);

    // Collect SOA file sizes
    std::vector<std::pair<std::string, double>> soa_file_sizes;
    std::vector<std::string> soa_files = {
        "./output/soa_event_all.root",
        "./output/soa_event_perData.root",
        "./output/soa_event_perGroup.root",
        "./output/soa_spill_all.root",
        "./output/soa_spill_perData.root",
        "./output/soa_spill_perGroup.root",
        "./output/soa_topObject_perData.root",
        "./output/soa_topObject_perGroup.root",
        "./output/soa_element_perData.root",
        "./output/soa_element_perGroup.root"
    };
    for (const auto& f : soa_files) {
        auto tfile = TFile::Open(f.c_str(), "READ");
        if (tfile) {
            double sizeMB = tfile->GetSize() / (1024.0 * 1024.0);
            soa_file_sizes.emplace_back(f, sizeMB);
            tfile->Close();
        }
    }
    visualize_soa_file_sizes(soa_file_sizes);

    // Add comparison visualizations
    visualize_comparison_writer_results(aos_writer_results, soa_writer_results);
    visualize_comparison_reader_results(aos_reader_results, soa_reader_results);
    visualize_comparison_file_sizes(aos_file_sizes, soa_file_sizes);

    // auto aos_scaling = benchmarkAOSScaling(32, 3);
    // visualize_aos_scaling(aos_scaling);

    return 0;
}

/*
cd RNTuple-I-O-Config && \
rm -rf build && \
mkdir build && \
cd build && \
cmake .. && \
make && \
./tests/gen_test_rootfile && \
ctest -R test_split_range_by_clusters --output-on-failure && \
echo "Started at: $(date)" | tee ../experiments/log.txt && \
./hitwire 2>&1 | tee -a ../experiments/log.txt && \
rm -rf build && \
cd .. && cd ..
*/