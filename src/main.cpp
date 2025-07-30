#include "updatedVisualization.hpp"
#include "updatedHitWireReaders.hpp"
#include "HitWireReaders.hpp"
#include "HitWireStat.hpp"
#include <TSystem.h>
#include <TROOT.h>
#include <thread>

#include "ReaderResult.hpp"

#include "visualization.hpp"

#include "WriterResult.hpp"

#include <iostream>
#include <filesystem>

#include "updatedHitWireWriters.hpp"
#include <TFile.h>

// Enable ROOT thread safety and implicit multi-threading

int main() {
    ROOT::EnableThreadSafety();
    int nThreads = std::thread::hardware_concurrency();
    ROOT::EnableImplicitMT(nThreads);
    gSystem->Load("libWireDict");
    // auto writer_results = out(nThreads, 3);
    // std::cout << "Calling visualize_writer_results..." << std::endl;
    // visualize_writer_results(writer_results);

    // Collect file sizes
    // std::vector<std::pair<std::string, double>> fileSizes;
    // std::string outputDir = "./output";
    // std::vector<std::string> fileNames = {
    //     "vector.root", "individual.root", "VertiSplit_vector.root", "VertiSplit_individual.root",
    //     "HoriSpill_vector.root", "HoriSpill_individual.root", "vector_dict.root", "individual_dict.root",
    //     "VertiSplit_vector_dict.root", "VertiSplit_individual_dict.root", "HoriSpill_vector_dict.root",
    //     "HoriSpill_individual_dict.root", "vector_of_individuals.root"
    // };
    // for (size_t i = 0; i < writer_results.size(); ++i) {
    //     std::string filePath = outputDir + "/" + fileNames[i];
    //     if (std::filesystem::exists(filePath)) {
    //         double sizeMB = static_cast<double>(std::filesystem::file_size(filePath)) / (1024 * 1024);
    //         fileSizes.emplace_back(writer_results[i].label, sizeMB);
    //     } else {
    //         fileSizes.emplace_back(writer_results[i].label, 0.0);
    //     }
    // }
    // visualize_file_sizes(fileSizes);

    // printEntryCounts(); // New table for entry counts
    // printFileStats(); // Existing file statistics
    // auto reader_results = in(nThreads, 3);
    // visualize_reader_results(reader_results);

    // New scaling benchmark and visualization
    // auto scalingData = benchmarkScaling(32, 3); // maxThreads=32, iter=3
    // visualize_scaling(scalingData);

    // Remove temporary calls
    // Add AOS visualization - use separate AOS-only functions
    auto aos_writer_results = updatedOut(nThreads, 3);
    // Filter to get only AOS results
    std::vector<WriterResult> aos_only_writer_results;
    for (const auto& result : aos_writer_results) {
        if (result.label.find("AOS_") == 0) {
            aos_only_writer_results.push_back(result);
        }
    }
    visualize_aos_writer_results(aos_only_writer_results);
    
    auto aos_reader_results = updatedIn(nThreads, 3);
    // Filter to get only AOS results
    std::vector<ReaderResult> aos_only_reader_results;
    for (const auto& result : aos_reader_results) {
        if (result.label.find("AOS_") == 0) {
            aos_only_reader_results.push_back(result);
        }
    }
    visualize_aos_reader_results(aos_only_reader_results);

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
    visualize_comparison_writer_results(aos_only_writer_results, soa_writer_results);
    visualize_comparison_reader_results(aos_only_reader_results, soa_reader_results);
    visualize_comparison_file_sizes(aos_file_sizes, soa_file_sizes);

    // auto aos_scaling = benchmarkAOSScaling(32, 3);
    // visualize_aos_scaling(aos_scaling);

    return 0;
}

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..


/*
cd project && \
rm -rf build && \
mkdir build && \
cd build && \
cmake .. && \
make && \
./tests/gen_test_rootfile && \
ctest -R test_split_range_by_clusters --output-on-failure && \
./hitwire && \
rm -rf build && \
cd ..
*/