#pragma once

void generateAndWriteHitWireDataSoA(int eventCount, int fieldSize);
void generateAndWriteSplitHitAndWireDataSoA(int eventCount, int fieldSize);
void generateAndWriteSpilHitAndWireDataSoA(int eventCount, int spilCount, int fieldSize);
void generateAndWriteHitWireDataAoS(int eventCount, int fieldSize);
void generateAndWriteSplitHitAndWireDataAoS(int eventCount, int fieldSize);
void generateAndWriteSpilHitAndWireDataAoS(int eventCount, int spilCount, int fieldSize); 