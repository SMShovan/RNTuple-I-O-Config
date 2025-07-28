#include "HitWireWriters.hpp"
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

// Enable ROOT thread safety and implicit multi-threading

int main() {
    ROOT::EnableThreadSafety();
    int nThreads = std::thread::hardware_concurrency();
    ROOT::EnableImplicitMT(nThreads);
    gSystem->Load("libWireDict");
    auto writer_results = out(nThreads, 3);
    std::cout << "Calling visualize_writer_results..." << std::endl;
    visualize_writer_results(writer_results);

    // Collect file sizes
    std::vector<std::pair<std::string, double>> fileSizes;
    std::string outputDir = "./output";
    std::vector<std::string> fileNames = {
        "vector.root", "individual.root", "VertiSplit_vector.root", "VertiSplit_individual.root",
        "HoriSpill_vector.root", "HoriSpill_individual.root", "vector_dict.root", "individual_dict.root",
        "VertiSplit_vector_dict.root", "VertiSplit_individual_dict.root", "HoriSpill_vector_dict.root",
        "HoriSpill_individual_dict.root", "vector_of_individuals.root"
    };
    for (size_t i = 0; i < writer_results.size(); ++i) {
        std::string filePath = outputDir + "/" + fileNames[i];
        if (std::filesystem::exists(filePath)) {
            double sizeMB = static_cast<double>(std::filesystem::file_size(filePath)) / (1024 * 1024);
            fileSizes.emplace_back(writer_results[i].label, sizeMB);
        } else {
            fileSizes.emplace_back(writer_results[i].label, 0.0);
        }
    }
    visualize_file_sizes(fileSizes);

    printFileStats(); // Print file statistics
    auto reader_results = in(nThreads, 3);
    visualize_reader_results(reader_results);

    // New scaling benchmark and visualization
    auto scalingData = benchmarkScaling(32, 3); // maxThreads=32, iter=3
    visualize_scaling(scalingData);

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