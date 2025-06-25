#pragma once
#include "Hit.hpp"
#include "Wire.hpp"
#include <random>

HitSoA generateRandomHitSoA(long long eventID, int fieldSize, std::mt19937& rng);
WireSoA generateRandomWireSoA(long long eventID, int fieldSize, std::mt19937& rng);
HitAoS generateRandomHitAoS(long long eventID, std::mt19937& rng);
WireAoS generateRandomWireAoS(long long eventID, int nROIs, std::mt19937& rng); 