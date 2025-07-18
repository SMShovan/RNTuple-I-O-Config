#include "HitWireWriters.hpp"
#include "HitWireReaders.hpp"
#include <TSystem.h>
#include <TROOT.h>
#include <thread>

// Enable ROOT thread safety and implicit multi-threading

int main() {
    ROOT::EnableThreadSafety();
    int nThreads = std::thread::hardware_concurrency();
    ROOT::EnableImplicitMT(nThreads);
    gSystem->Load("libWireDict");
    out(nThreads);
    in(nThreads);
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