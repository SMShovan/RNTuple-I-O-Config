#include "HitWireWriters.hpp"

int main() {
    int EventCount = 1024;
    int FieldSize = 64; //count hit and wirecount
    int SpilCount = 32;

    generateAndWriteHitWireDataSoA(EventCount, FieldSize, "./hitwire/HitWireSoA.root");
    //generateAndWriteSplitHitAndWireDataSoA(EventCount, FieldSize, "./hitwire/split_hitwire.root");
    //generateAndWriteSpilHitAndWireDataSoA(EventCount, SpilCount, FieldSize, "./hitwire/hitspils/hits_spil_all_SoA.root");
    //generateAndWriteHitWireDataAoS(EventCount, FieldSize, "./hitwire/HitWireAoS.root");
    //generateAndWriteSplitHitAndWireDataAoS(EventCount, FieldSize, "./hitwire/split_hitwireAoS.root");
    //generateAndWriteSpilHitAndWireDataAoS(EventCount, SpilCount, FieldSize, "./hitwire/hitspils/hits_spil_all_AoS.root");
    return 0;
} 

// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire
// cd project && mkdir -p build && cd build && cmake .. && make && cd .. && ./build/hitwire && rm -rf ./build && cd ..