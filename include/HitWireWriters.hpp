#pragma once
#include <string>

void generateAndWriteHitWireDataVector(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataVector(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataVector(int eventCount, int spilCount, int fieldSize, const std::string& fileName);
void generateAndWriteHitWireDataIndividual(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSplitHitAndWireDataIndividual(int eventCount, int fieldSize, const std::string& fileName);
void generateAndWriteSpilHitAndWireDataIndividual(int eventCount, int spilCount, int fieldSize, const std::string& fileName); 