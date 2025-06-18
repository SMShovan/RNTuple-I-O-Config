#pragma once
#include <vector>

class HitSoA {
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

    std::vector<unsigned int>& getChannel() { return fChannel; }
    std::vector<int>& getView() { return fView; }
    std::vector<int>& getStartTick() { return fStartTick; }
    std::vector<int>& getEndTick() { return fEndTick; }
    std::vector<float>& getPeakTime() { return fPeakTime; }
    std::vector<float>& getSigmaPeakTime() { return fSigmaPeakTime; }
    std::vector<float>& getRMS() { return fRMS; }
    std::vector<float>& getPeakAmplitude() { return fPeakAmplitude; }
    std::vector<float>& getSigmaPeakAmplitude() { return fSigmaPeakAmplitude; }
    std::vector<float>& getROISummedADC() { return fROISummedADC; }
    std::vector<float>& getHitSummedADC() { return fHitSummedADC; }
    std::vector<float>& getIntegral() { return fIntegral; }
    std::vector<float>& getSigmaIntegral() { return fSigmaIntegral; }
    std::vector<short int>& getMultiplicity() { return fMultiplicity; }
    std::vector<short int>& getLocalIndex() { return fLocalIndex; }
    std::vector<float>& getGoodnessOfFit() { return fGoodnessOfFit; }
    std::vector<int>& getNDF() { return fNDF; }
    std::vector<int>& getSignalType() { return fSignalType; }
    std::vector<int>& getWireID_Cryostat() { return fWireID_Cryostat; }
    std::vector<int>& getWireID_TPC() { return fWireID_TPC; }
    std::vector<int>& getWireID_Plane() { return fWireID_Plane; }
    std::vector<int>& getWireID_Wire() { return fWireID_Wire; }
};

// AoS version for a single hit
struct HitAoS {
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
};

// AoS generator declaration
HitAoS generateRandomHitAoS(long long eventID); 