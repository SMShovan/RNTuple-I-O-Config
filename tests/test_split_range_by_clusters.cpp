#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <vector>
#include <utility>
#include <cstddef>

namespace ROOT {
namespace Experimental {
class ClusterDescriptor {
public:
    virtual ~ClusterDescriptor() = default;
    virtual std::size_t GetFirstEntryIndex() const = 0;
    virtual std::size_t GetNEntries() const = 0;
};

class Descriptor {
public:
    virtual ~Descriptor() = default;
    virtual int GetNClusters() const = 0;
    virtual const ClusterDescriptor& GetClusterDescriptor(std::size_t) const = 0;
};

class RNTupleReader {
public:
    virtual ~RNTupleReader() = default;
    virtual const Descriptor& GetDescriptor() const = 0;
};
} // namespace Experimental
} // namespace ROOT

// Test-only implementation for the mock classes
std::vector<std::pair<std::size_t, std::size_t>> split_range_by_clusters(ROOT::Experimental::RNTupleReader* reader, int nChunks) {
    const auto& desc = reader->GetDescriptor();
    auto nClusters = desc.GetNClusters();
    if (nClusters == 0) {
        return {};
    }
    int clusters_per_chunk = nClusters / nChunks;
    int remainder = nClusters % nChunks;
    std::vector<std::pair<std::size_t, std::size_t>> chunks;
    std::uint64_t current_cid = 0;
    for (int i = 0; i < nChunks; ++i) {
        int num_clusters = clusters_per_chunk + (i < remainder ? 1 : 0);
        if (num_clusters == 0) continue;
        auto start_cid = current_cid;
        current_cid += num_clusters;
        const auto& start_desc = desc.GetClusterDescriptor(start_cid);
        const auto& end_desc = desc.GetClusterDescriptor(current_cid - 1);
        std::size_t start = start_desc.GetFirstEntryIndex();
        std::size_t end = end_desc.GetFirstEntryIndex() + end_desc.GetNEntries();
        chunks.emplace_back(start, end);
    }
    return chunks;
}

using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::NiceMock;

class MockClusterDescriptor : public ROOT::Experimental::ClusterDescriptor {
public:
    MOCK_METHOD(std::size_t, GetFirstEntryIndex, (), (const, override));
    MOCK_METHOD(std::size_t, GetNEntries, (), (const, override));
};

class MockDescriptor : public ROOT::Experimental::Descriptor {
public:
    MOCK_METHOD(int, GetNClusters, (), (const, override));
    MOCK_METHOD(const ROOT::Experimental::ClusterDescriptor&, GetClusterDescriptor, (std::size_t), (const, override));
};

class MockRNTupleReader : public ROOT::Experimental::RNTupleReader {
public:
    MOCK_METHOD(const ROOT::Experimental::Descriptor&, GetDescriptor, (), (const, override));
};

TEST(SplitRangeByClustersTest, SimpleMockedCase) {
    // Set up mocks
    auto cluster0 = new NiceMock<MockClusterDescriptor>();
    auto cluster1 = new NiceMock<MockClusterDescriptor>();
    auto cluster2 = new NiceMock<MockClusterDescriptor>();
    EXPECT_CALL(*cluster0, GetFirstEntryIndex()).WillRepeatedly(Return(0));
    EXPECT_CALL(*cluster0, GetNEntries()).WillRepeatedly(Return(2));
    EXPECT_CALL(*cluster1, GetFirstEntryIndex()).WillRepeatedly(Return(2));
    EXPECT_CALL(*cluster1, GetNEntries()).WillRepeatedly(Return(3));
    EXPECT_CALL(*cluster2, GetFirstEntryIndex()).WillRepeatedly(Return(5));
    EXPECT_CALL(*cluster2, GetNEntries()).WillRepeatedly(Return(5));

    auto descriptor = new NiceMock<MockDescriptor>();
    EXPECT_CALL(*descriptor, GetNClusters()).WillRepeatedly(Return(3));
    EXPECT_CALL(*descriptor, GetClusterDescriptor(0)).WillRepeatedly(ReturnRef(*cluster0));
    EXPECT_CALL(*descriptor, GetClusterDescriptor(1)).WillRepeatedly(ReturnRef(*cluster1));
    EXPECT_CALL(*descriptor, GetClusterDescriptor(2)).WillRepeatedly(ReturnRef(*cluster2));

    auto reader = new NiceMock<MockRNTupleReader>();
    EXPECT_CALL(*reader, GetDescriptor()).WillRepeatedly(ReturnRef(*descriptor));

    // Call the function under test
    auto chunks = split_range_by_clusters(reader, 2);
    // Clean up
    delete cluster0;
    delete cluster1;
    delete cluster2;
    delete descriptor;
    delete reader;

    // Check the result (should split clusters into 2 chunks)
    ASSERT_EQ(chunks.size(), 2);
    EXPECT_EQ(chunks[0], std::make_pair(0ul, 5ul)); // clusters 0 and 1
    EXPECT_EQ(chunks[1], std::make_pair(5ul, 10ul)); // cluster 2
} 