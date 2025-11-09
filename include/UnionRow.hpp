#pragma once
#include "Hit.hpp"
#include "Wire.hpp"
#include <Rtypes.h>
#include <Rtypes.h>

// Union-like row type for AOS union-of-elements ntuples
struct AOSUnionRow {
    unsigned int EventID;
    unsigned char recordType; // 0=Hit, 1=Wire, 2=ROI
    unsigned int WireID;      // valid when recordType==2 (ROI)
    HitIndividual hit;
    WireBase      wire;
    FlatROI       roi;
    ClassDefNV(AOSUnionRow, 1);
};


