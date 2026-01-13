#pragma once
#include <vector>
#include <TObject.h>

/**
 * @class HitVector
 * @brief Container for multiple hits in an event, using vectors for attributes.
 *
 * Stores per-hit data like channels, ticks, amplitudes across events.
 * Supports ROOT I/O via ClassDef and getters for each field.
 */
class HitVector{
public:
    long long EventID;
    std::vector<unsigned int> fChannel;
    std::vector<int> fView;
    std::vector<int> fStartTick;
    std::vector<int> fEndTick;
    std::vector<float> fPeakTime;
    std::vector<float> fSigmaPeakTime;
    std::vector<float> fRMS;
    std::vector<float> fPeakAmplitude;
    std::vector<float> fSigmaPeakAmplitude;
    std::vector<float> fROISummedADC;
    std::vector<float> fHitSummedADC;
    std::vector<float> fIntegral;
    std::vector<float> fSigmaIntegral;
    std::vector<short int> fMultiplicity;
    std::vector<short int> fLocalIndex;
    std::vector<float> fGoodnessOfFit;
    std::vector<int> fNDF;
    std::vector<int> fSignalType;
    std::vector<int> fWireID_Cryostat;
    std::vector<int> fWireID_TPC;
    std::vector<int> fWireID_Plane;
    std::vector<int> fWireID_Wire;

    HitVector() = default;
    
    std::vector<unsigned int>& getChannel() { return fChannel; }
    const std::vector<unsigned int>& getChannel() const { return fChannel; }
    std::vector<int>& getView() { return fView; }
    const std::vector<int>& getView() const { return fView; }
    std::vector<int>& getStartTick() { return fStartTick; }
    const std::vector<int>& getStartTick() const { return fStartTick; }
    std::vector<int>& getEndTick() { return fEndTick; }
    const std::vector<int>& getEndTick() const { return fEndTick; }
    std::vector<float>& getPeakTime() { return fPeakTime; }
    const std::vector<float>& getPeakTime() const { return fPeakTime; }
    std::vector<float>& getSigmaPeakTime() { return fSigmaPeakTime; }
    const std::vector<float>& getSigmaPeakTime() const { return fSigmaPeakTime; }
    std::vector<float>& getRMS() { return fRMS; }
    const std::vector<float>& getRMS() const { return fRMS; }
    std::vector<float>& getPeakAmplitude() { return fPeakAmplitude; }
    const std::vector<float>& getPeakAmplitude() const { return fPeakAmplitude; }
    std::vector<float>& getSigmaPeakAmplitude() { return fSigmaPeakAmplitude; }
    const std::vector<float>& getSigmaPeakAmplitude() const { return fSigmaPeakAmplitude; }
    std::vector<float>& getROISummedADC() { return fROISummedADC; }
    const std::vector<float>& getROISummedADC() const { return fROISummedADC; }
    std::vector<float>& getHitSummedADC() { return fHitSummedADC; }
    const std::vector<float>& getHitSummedADC() const { return fHitSummedADC; }
    std::vector<float>& getIntegral() { return fIntegral; }
    const std::vector<float>& getIntegral() const { return fIntegral; }
    std::vector<float>& getSigmaIntegral() { return fSigmaIntegral; }
    const std::vector<float>& getSigmaIntegral() const { return fSigmaIntegral; }
    std::vector<short int>& getMultiplicity() { return fMultiplicity; }
    const std::vector<short int>& getMultiplicity() const { return fMultiplicity; }
    std::vector<short int>& getLocalIndex() { return fLocalIndex; }
    const std::vector<short int>& getLocalIndex() const { return fLocalIndex; }
    std::vector<float>& getGoodnessOfFit() { return fGoodnessOfFit; }
    const std::vector<float>& getGoodnessOfFit() const { return fGoodnessOfFit; }
    std::vector<int>& getNDF() { return fNDF; }
    const std::vector<int>& getNDF() const { return fNDF; }
    std::vector<int>& getSignalType() { return fSignalType; }
    const std::vector<int>& getSignalType() const { return fSignalType; }
    std::vector<int>& getWireID_Cryostat() { return fWireID_Cryostat; }
    const std::vector<int>& getWireID_Cryostat() const { return fWireID_Cryostat; }
    std::vector<int>& getWireID_TPC() { return fWireID_TPC; }
    const std::vector<int>& getWireID_TPC() const { return fWireID_TPC; }
    std::vector<int>& getWireID_Plane() { return fWireID_Plane; }
    const std::vector<int>& getWireID_Plane() const { return fWireID_Plane; }
    std::vector<int>& getWireID_Wire() { return fWireID_Wire; }
    const std::vector<int>& getWireID_Wire() const { return fWireID_Wire; }
    ClassDef(HitVector, 3)
};

/**
 * @class HitIndividual
 * @brief Represents a single hit with individual attributes.
 *
 * Used for non-vectorized storage; includes all hit fields as scalars.
 * Supports ROOT I/O via ClassDef.
 */
struct HitIndividual {
    long long EventID;
    unsigned int fChannel;
    int fView;
    int fStartTick;
    int fEndTick;
    float fPeakTime;
    float fSigmaPeakTime;
    float fRMS;
    float fPeakAmplitude;
    float fSigmaPeakAmplitude;
    float fROISummedADC;
    float fHitSummedADC;
    float fIntegral;
    float fSigmaIntegral;
    short int fMultiplicity;
    short int fLocalIndex;
    float fGoodnessOfFit;
    int fNDF;
    int fSignalType;
    int fWireID_Cryostat;
    int fWireID_TPC;
    int fWireID_Plane;
    int fWireID_Wire;
    HitIndividual() = default;
    
    ClassDef(HitIndividual, 3)
};

// Individual generator declaration
HitIndividual generateRandomHitIndividual(long long eventID); 

struct SOAHitVector {
    std::vector<long long> EventIDs;
    std::vector<unsigned int> fChannel;
    std::vector<int> fView;
    std::vector<int> fStartTick;
    std::vector<int> fEndTick;
    std::vector<float> fPeakTime;
    std::vector<float> fSigmaPeakTime;
    std::vector<float> fRMS;
    std::vector<float> fPeakAmplitude;
    std::vector<float> fSigmaPeakAmplitude;
    std::vector<float> fROISummedADC;
    std::vector<float> fHitSummedADC;
    std::vector<float> fIntegral;
    std::vector<float> fSigmaIntegral;
    std::vector<short int> fMultiplicity;
    std::vector<short int> fLocalIndex;
    std::vector<float> fGoodnessOfFit;
    std::vector<int> fNDF;
    std::vector<int> fSignalType;
    std::vector<int> fWireID_Cryostat;
    std::vector<int> fWireID_TPC;
    std::vector<int> fWireID_Plane;
    std::vector<int> fWireID_Wire;
    ClassDef(SOAHitVector, 2);
};

// Ensure SOAHit is present (scalar fields, no vectors)
struct SOAHit {
    long long EventID;
    unsigned int fChannel;
    int fView;
    int fStartTick;
    int fEndTick;
    float fPeakTime;
    float fSigmaPeakTime;
    float fRMS;
    float fPeakAmplitude;
    float fSigmaPeakAmplitude;
    float fROISummedADC;
    float fHitSummedADC;
    float fIntegral;
    float fSigmaIntegral;
    short int fMultiplicity;
    short int fLocalIndex;
    float fGoodnessOfFit;
    int fNDF;
    int fSignalType;
    int fWireID_Cryostat;
    int fWireID_TPC;
    int fWireID_Plane;
    int fWireID_Wire;
    ClassDef(SOAHit, 2);
}; 