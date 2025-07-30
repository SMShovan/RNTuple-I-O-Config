#include <vector>
#include <map>
#include <utility>
#include <iostream>
#include <iomanip>
#include "WriterResult.hpp"
#include "ReaderResult.hpp"

// Simple table for writers
void visualize_aos_writer_results(const std::vector<WriterResult>& results) {
    std::cout << std::left << std::setw(32) << "Writer" << std::setw(16) << "Avg (ms)" << std::setw(16) << "StdDev (ms)" << std::endl;
    for (const auto& r : results) {
        std::cout << std::left << std::setw(32) << r.label << std::setw(16) << r.avg << std::setw(16) << r.stddev << std::endl;
    }
}

// Similar for readers
void visualize_aos_reader_results(const std::vector<ReaderResult>& results) {
    std::cout << std::left << std::setw(32) << "Reader" << std::setw(16) << "Avg (ms)" << std::setw(16) << "StdDev (ms)" << std::endl;
    for (const auto& r : results) {
        std::cout << std::left << std::setw(32) << r.label << std::setw(16) << r.warmAvg << std::setw(16) << r.warmStddev << std::endl;
    }
}

// File sizes table
void visualize_aos_file_sizes(const std::vector<std::pair<std::string, double>>& sizes) {
    std::cout << std::left << std::setw(32) << "File" << std::setw(16) << "Size (MB)" << std::endl;
    for (const auto& s : sizes) {
        std::cout << std::left << std::setw(32) << s.first << std::setw(16) << s.second << std::endl;
    }
}

// Scaling stub (cout for now)
void visualize_aos_scaling(const std::map<std::string, std::vector<std::pair<int, double>>>& data) {
    for (const auto& d : data) {
        std::cout << d.first << std::endl;
        for (const auto& p : d.second) {
            std::cout << "  Threads: " << p.first << ", Avg: " << p.second << std::endl;
        }
    }
}