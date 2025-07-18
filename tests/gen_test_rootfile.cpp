#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>
#include <memory>
#include <string>

int main() {
    // Create a simple ntuple with a single int column
    auto model = ROOT::RNTupleModel::Create();
    auto val = model->MakeField<int>("value");

    // Set up write options to force small clusters
    ROOT::RNTupleWriteOptions options;
    options.SetApproxZippedClusterSize(1024); // small, but large enough for ROOT

    auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "hits", "test_file.root", options);

    // Write 1000 entries to ensure multiple clusters
    for (int i = 0; i < 1000; ++i) {
        *val = i;
        writer->Fill();
    }
    // File is closed when writer goes out of scope
    return 0;
}