#pragma once
#include <string>

void generateAndWriteHitWireDataSoA(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataSoA(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataSoA(int eventCount, int spilCount, int fieldSize, const std::string& fileName);
void generateAndWriteHitWireDataAoS(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataAoS(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataAoS(int eventCount, int spilCount, int fieldSize, const std::string& fileName); 