#include "HitWireWriters.hpp"
#include "HitWireReaders.hpp"
#include <TSystem.h>

int main() {
    gSystem->Load("libWireDict");
    out();
    in();
    return 0;
}

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..