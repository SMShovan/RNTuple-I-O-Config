#pragma once
#include "Hit.hpp"
#include "HitWireWriterHelpers.hpp" // for WireBase, FlatROI
#include <Rtypes.h>
#include <vector>
//
// Batch row type for topObject_allDataProduct (AOS)
// Each row may optionally contain a hit and/or a wire with its ROIs.
//
struct AOSTopBatchRow {
    unsigned int EventID;
    bool hasHit;
    HitIndividual hit;
    bool hasWire;
    WireBase wire;
    std::vector<FlatROI> rois;
    ClassDefNV(AOSTopBatchRow, 1);
};


