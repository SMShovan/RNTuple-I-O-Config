#include "HitWireWriters.hpp"

int main() {
    int EventCount = 10;
    int FieldSize = 6;
    int SpilCount = 3;

    generateAndWriteHitWireDataSoA(EventCount, FieldSize);
    generateAndWriteSplitHitAndWireDataSoA(EventCount, FieldSize);
    generateAndWriteSpilHitAndWireDataSoA(EventCount, SpilCount, FieldSize);
    generateAndWriteHitWireDataAoS(EventCount, FieldSize);
    generateAndWriteSplitHitAndWireDataAoS(EventCount, FieldSize);
    generateAndWriteSpilHitAndWireDataAoS(EventCount, SpilCount, FieldSize);
    return 0;
} 

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..