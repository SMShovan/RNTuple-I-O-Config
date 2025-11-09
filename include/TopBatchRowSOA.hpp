#pragma once
#include "Hit.hpp"   // SOAHit, FlatSOAROI
#include "Wire.hpp"  // SOAWireBase, SOAROI
#include <Rtypes.h>
#include <vector>
//
// Batch row type for topObject_allDataProduct (SOA)
// Each row may optionally contain a hit and/or a wire with its ROIs.
//
struct SOATopBatchRow {
    unsigned int EventID;
    bool hasHit;
    SOAHit hit;
    bool hasWire;
    SOAWireBase wire;
    std::vector<SOAROI> rois;
    ClassDefNV(SOATopBatchRow, 1);
};


