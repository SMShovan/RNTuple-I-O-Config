#pragma once
#include "Hit.hpp"
#include "Wire.hpp"
#include <random>

HitVector generateRandomHitVector(long long eventID, int fieldSize, std::mt19937& rng);
WireVector generateRandomWireVector(long long eventID, int fieldSize, std::mt19937& rng);
HitIndividual generateRandomHitIndividual(long long eventID, std::mt19937& rng);
WireIndividual generateRandomWireIndividual(long long eventID, int nROIs, std::mt19937& rng); 