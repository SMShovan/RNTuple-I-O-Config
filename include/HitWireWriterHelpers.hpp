#ifndef HITWIREWRITERHELPERS_HPP
#define HITWIREWRITERHELPERS_HPP

#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RFieldToken.hxx>
#include <unordered_map>
#include <utility> // for std::pair

// Forward declarations if needed (e.g., for custom types like WireVector)
class WireVector;

// Helper for creating vertically split hit model and tokens
auto CreateVertiSplitHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>>;

// Helper for creating wire model and token
auto CreateWireModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Helper for HitVector model and token
auto CreateHitVectorModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Helper for WireVector model and token
auto CreateWireVectorModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Helper for HoriSpill hit model and tokens
auto CreateHoriSpillHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>>;

// Helper for individual hit model and tokens
auto CreateIndividualHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>>;

// Helper for individual wire model and token
auto CreateIndividualWireModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Helper for VertiSplit individual hit model and tokens
auto CreateVertiSplitIndividualHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>>;

// Helper for VertiSplit individual wire model and token
auto CreateIndividualWireModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Helper for HoriSpill individual hit model and tokens
auto CreateHoriSpillIndividualHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>>;

// Helper for HoriSpill individual wire model and token (reuse CreateIndividualWireModelAndToken)

// Forward declarations for other dependencies in work func (add as needed)
#include <ROOT/RNTupleFillContext.hxx>
#include <ROOT/RRawPtrWriteEntry.hxx>
#include <mutex>


// Helper for the work function
double RunVertiSplitWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for the work function
double RunHitWireVectorWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for the work function
double RunHoriSpillWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
);

// Helper for the work function
double RunIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for the work function
double RunVertiSplitIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for the work function
double RunHoriSpillIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
);

// Helper for HitWire Vector Dict model and token
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateHitWireVectorDictModelAndToken();
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateWireVectorDictModelAndToken();

double RunHitWireVectorDictWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for HitWire Individual Dict model and token
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateHitWireIndividualDictModelAndToken();
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateWireIndividualDictModelAndToken();

double RunHitWireIndividualDictWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for VertiSplit HitWire Vector Dict model and tokens
std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> CreateVertiSplitHitWireVectorDictModelAndTokens();
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateWireVectorDictModelAndToken();

double RunVertiSplitHitWireVectorDictWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for VertiSplit HitWire Individual Dict model and tokens
std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> CreateVertiSplitHitWireIndividualDictModelAndTokens();
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateWireIndividualDictModelAndToken();

double RunVertiSplitHitWireIndividualDictWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Helper for VertiSplit HitWire Individual Dict wire model and token
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateVertiSplitWireIndividualDictModelAndToken();

// Helper for HoriSpill HitWire Vector Dict model and tokens
std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> CreateHoriSpillHitWireVectorDictModelAndTokens();
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateHoriSpillWireVectorDictModelAndToken();

double RunHoriSpillHitWireVectorDictWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
);

// Helper for HoriSpill HitWire Individual Dict model and tokens
std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> CreateHoriSpillHitWireIndividualDictModelAndTokens();
std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken> CreateHoriSpillWireIndividualDictModelAndToken();

double RunHoriSpillHitWireIndividualDictWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
);

// Helper for HitWire Vector Of Individuals model and tokens
std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> CreateHitWireVectorOfIndividualsModelAndTokens();
std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>> CreateWireVectorOfIndividualsModelAndTokens();

double RunHitWireVectorOfIndividualsWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    const std::unordered_map<std::string, ROOT::RFieldToken>& wireTokens,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Dict-specific model creation for individual hit
auto CreateDictIndividualHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Dict-specific model creation for individual wire
auto CreateDictIndividualWireModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Dict-specific work function for individual dict
double RunDictIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Dict-specific verti-split hit model and token (using single class field)
auto CreateDictVertiSplitHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Dict-specific work function for verti-split vector (binding single pointer)
double RunDictVertiSplitWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Dict-specific verti-split individual hit model and token (single class field)
auto CreateDictVertiSplitIndividualHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Dict-specific work function for verti-split individual
double RunDictVertiSplitIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

// Dict-specific hori-spill hit model and token (single class field)
auto CreateDictHoriSpillHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Dict-specific work function for hori-spill vector
double RunDictHoriSpillWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
);

// Dict-specific hori-spill individual hit model and token (single class field)
auto CreateDictHoriSpillIndividualHitModelAndToken() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, ROOT::RFieldToken>;

// Dict-specific work function for hori-spill individual
double RunDictHoriSpillIndividualWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    ROOT::RFieldToken hitToken,
    ROOT::RFieldToken wireToken,
    std::mutex& mutex,
    int totalEntries, int nThreads, int numHoriSpills, int adjustedHitsPerEvent, int adjustedWiresPerEvent, int roisPerWire
);

// Model creation for vector-of-individuals hit
auto CreateVectorOfIndividualsHitModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>>;

// Model creation for vector-of-individuals wire
auto CreateVectorOfIndividualsWireModelAndTokens() -> std::pair<std::unique_ptr<ROOT::RNTupleModel>, std::unordered_map<std::string, ROOT::RFieldToken>>;

// Work function for vector-of-individuals
double RunVectorOfIndividualsWorkFunc(
    int first, int last, unsigned seed,
    ROOT::Experimental::RNTupleFillContext& hitContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& hitEntry,
    ROOT::Experimental::RNTupleFillContext& wireContext,
    ROOT::Experimental::Detail::RRawPtrWriteEntry& wireEntry,
    const std::unordered_map<std::string, ROOT::RFieldToken>& hitTokens,
    const std::unordered_map<std::string, ROOT::RFieldToken>& wireTokens,
    std::mutex& mutex,
    int numEvents, int nThreads, int hitsPerEvent, int wiresPerEvent, int roisPerWire
);

#endif // HITWIREWRITERHELPERS_HPP 