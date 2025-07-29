# RNTuple Entry Counts Report

## Key Variable Values
These are the default parameters used in the benchmarks:
- `numEvents`: 1000 (total events processed)
- `hitsPerEvent`: 1000 (hits generated per event)
- `wiresPerEvent`: 100 (wires generated per event)
- `numHoriSpills`: 10 (spills for horizontal spill configurations)
- `roisPerWire`: 10 (regions of interest per wire)

Total generated data: 1,000,000 hits and 100,000 wires (with 1,000,000 ROIs).

## Entry Counts Table

| Writer                          | Hits Entries | Wires Entries |
|---------------------------------|--------------|---------------|
| Hit/Wire Vector                 | 1000         | 1000          |
| Hit/Wire Individual             | 1000000      | 100000        |
| VertiSplit-Vector               | 1000         | 1000          |
| VertiSplit-Individual           | 1000000      | 100000        |
| HoriSpill-Vector                | 10000        | 10000         |
| HoriSpill-Individual            | 1000000      | 100000        |
| Vector-Dict                     | 1000         | 1000          |
| Individual-Dict                 | 1000000      | 100000        |
| VertiSplit-Vector-Dict          | 1000         | 1000          |
| VertiSplit-Individual-Dict      | 1000000      | 100000        |
| HoriSpill-Vector-Dict           | 10000        | 10000         |
| HoriSpill-Individual-Dict       | 1000000      | 100000        |
| Vector-of-Individuals           | 1000         | 1000          |

## Rationale for Entry Counts by Writer

- **Hit/Wire Vector**: 1000 entries (one per event); aggregates all hits/wires into vectors per entry for efficient batch storage.
- **Hit/Wire Individual**: 1,000,000/100,000 entries (one per hit/wire); granular for fine queries but increases overhead.
- **VertiSplit-Vector**: Same as Vector (1000); vertical splitting optimizes field storage but keeps entry count event-based.
- **VertiSplit-Individual**: Same as Individual (1M/100K); combines splitting with per-item entries for optimized access.
- **HoriSpill-Vector**: 10,000 entries (events × spills); spills simulate data bursts, multiplying entries for realism.
- **HoriSpill-Individual**: Same as Individual (1M/100K); spills affect generation but entry count remains per-hit/wire.
- **Vector-Dict**: Same as Vector (1000); dictionary adds type info without changing entry structure.
- **Individual-Dict**: Same as Individual (1M/100K); dictionary for complex types but per-item entries unchanged.
- **VertiSplit-Vector-Dict**: Same as VertiSplit-Vector (1000); combines splitting and dict for optimized typed storage.
- **VertiSplit-Individual-Dict**: Same as VertiSplit-Individual (1M/100K); dict enhances type handling in split, per-item setup.
- **HoriSpill-Vector-Dict**: Same as HoriSpill-Vector (10K); dict with spills for typed, bursty data.
- **HoriSpill-Individual-Dict**: Same as HoriSpill-Individual (1M/100K); dict in spill context keeps per-item granularity.
- **Vector-of-Individuals**: 1000 entries (one per event); vectors hold individual objects, balancing aggregation and detail. 