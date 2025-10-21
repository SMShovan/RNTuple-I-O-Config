# HitWireProject

## Project Structure

- `src/`      — All C++ source (.cpp) files
- `include/`  — All C++ header (.hpp) files
- `build/`    — Build artifacts
- `CMakeLists.txt` — Build configuration
- `README.md` — This file

## Prerequisites

### For scisoftbuild02

1. **Install ROOT using spack:**
   ```sh
   # Install spack if not already available
   # Then install ROOT
   spack install root
   ```

2. **Set up environment:**
   ```sh
   source ~/spack/share/spack/setup-env.sh
   spack load root
   spack load vdt
   
   # Make the headers visible to the compiler invoked by cmake/rootcling
   export CPATH="$(spack location -i vdt)/include:${CPATH}"
   
   # (optional if you'll link against libvdt at runtime too)
   export LIBRARY_PATH="$(spack location -i vdt)/lib:${LIBRARY_PATH}"
   export LD_LIBRARY_PATH="$(spack location -i vdt)/lib:${LD_LIBRARY_PATH}"
   ```

## How to Build

### Complete Build and Test Process (scisoftbuild02)

```sh
cd RNTuple-I-O-Config && \
rm -rf build && \
mkdir build && \
cd build && \
cmake .. && \
make && \
./tests/gen_test_rootfile && \
ctest -R test_split_range_by_clusters --output-on-failure && \
echo "Started at: $(date)" | tee ../experiments/log.txt && \
./hitwire --writer-mask 1023 --reader-mask 1023 2>&1 | tee -a ../experiments/log.txt && \
rm -rf build && \
cd .. && cd ..
```

## Configuration

The project now uses centralized configuration parameters in `main.cpp`:

- `numEvents`: Number of events to process
- `hitsPerEvent`: Number of hits per event
- `wiresPerEvent`: Number of wires per event  
- `roisPerWire`: Number of ROIs per wire
- `numSpills`: Number of spills
- `kOutputDir`: Output directory for ROOT files

## Selecting Benchmarks via Bitmask

You can select which writer/reader benchmarks to run using bitmask flags.

- `--writer-mask N`: run only writer benchmarks whose bit is set in N
- `--reader-mask N`: run only reader benchmarks whose bit is set in N
- `--aos-only` / `--soa-only`: limit execution to a layout
- `--iter K`: number of iterations (first is cold, rest warm for readers)

Bit-to-benchmark mapping (index → benchmark):

0: event_allDataProduct  
1: event_perDataProduct  
2: event_perGroup  
3: spill_allDataProduct  
4: spill_perDataProduct  
5: spill_perGroup  
6: topObject_perDataProduct  
7: topObject_perGroup  
8: element_perDataProduct  
9: element_perGroup

Examples:

```sh
# Run only 1st and 2nd AOS writer/reader benchmarks
./hitwire --aos-only --writer-mask 3 --reader-mask 3

# Run 1st, 3rd, 5th SOA writer/reader benchmarks
./hitwire --soa-only --writer-mask 0x15 --reader-mask 0x15

# Run all AOS and SOA benchmarks (explicit mask)
./hitwire --writer-mask 1023 --reader-mask 1023

# Increase iterations to 3
./hitwire --iter 3 --writer-mask 3 --reader-mask 3
```

## Output

ROOT files are generated in the configured output directory with the following naming convention:
- AOS (Array of Structs): `aos_event_*.root`, `aos_spill_*.root`, etc.
- SOA (Struct of Arrays): `soa_event_*.root`, `soa_spill_*.root`, etc.
