#pragma once
#include "Hit.hpp"   // for SOAHit
#include "Wire.hpp"  // for SOAWireBase, FlatSOAROI
#include <Rtypes.h>

// Union-like row type for SOA union-of-elements ntuples
struct SOAUnionRow {
    unsigned int EventID;
    unsigned char recordType; // 0=Hit, 1=Wire, 2=ROI
    unsigned int WireID;      // valid when recordType==2 (ROI)
    SOAHit       hit;
    SOAWireBase  wire;
    FlatSOAROI   roi;
    ClassDefNV(SOAUnionRow, 1);
};


