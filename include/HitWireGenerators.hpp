#pragma once
#include "Hit.hpp"
#include "Wire.hpp"

HitSoA generateRandomHitSoA(long long eventID, int fieldSize);
WireSoA generateRandomWireSoA(long long eventID, int fieldSize);
HitAoS generateRandomHitAoS(long long eventID);
WireAoS generateRandomWireAoS(long long eventID); 