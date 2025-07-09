#include "HitWireWriters.hpp"
#include "HitWireReaders.hpp"
#include <TSystem.h>
#include <TROOT.h>
#include <thread>

// Enable ROOT thread safety and implicit multi-threading

int main() {
    ROOT::EnableThreadSafety();
    ROOT::EnableImplicitMT(std::thread::hardware_concurrency());
    gSystem->Load("libWireDict");
    out();
    in();
    return 0;
}

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..