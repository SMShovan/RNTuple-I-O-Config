#pragma once
#include "Hit.hpp"
#include "Wire.hpp"
#include <random>

HitVector generateRandomHitVector(long long eventID, int hitsPerEvent, std::mt19937& rng);
WireVector generateRandomWireVector(long long eventID, int wiresPerEvent, int roisPerWire, std::mt19937& rng);
WireVector generateRandomWireVector(long long eventID, int wiresPerEvent, std::mt19937& rng);
HitIndividual generateRandomHitIndividual(long long eventID, std::mt19937& rng);
WireIndividual generateRandomWireIndividual(long long eventID, int roisPerWire, std::mt19937& rng);
WireIndividual generateRandomWireIndividual(long long eventID, int /*dummyROIs*/, int numROIs, std::mt19937& rng); 