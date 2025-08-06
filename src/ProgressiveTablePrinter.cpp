#include "ProgressiveTablePrinter.hpp"
#include <iostream>
#include <iomanip>

// Specialization for WriterResult
template<>
void ProgressiveTablePrinter<WriterResult>::printRow(const WriterResult& result) {
    std::cout << std::left
              << std::setw(columnWidths[0]) << result.label;
    
    if (result.failed) {
        std::cout << std::setw(columnWidths[1]) << "FAILED"
                  << std::setw(columnWidths[2]) << "-";
        // Fill remaining columns with "-" if there are iteration columns
        for (size_t i = 3; i < columnWidths.size(); ++i) {
            std::cout << std::setw(columnWidths[i]) << "-";
        }
    } else {
        std::cout << std::setw(columnWidths[1]) << result.avg
                  << std::setw(columnWidths[2]) << result.stddev;
        
        // Print individual iteration times if available
        for (size_t i = 0; i < result.iterationTimes.size() && i + 3 < columnWidths.size(); ++i) {
            std::cout << std::setw(columnWidths[i + 3]) << result.iterationTimes[i];
        }
        
        // Fill remaining columns with "-" if we have fewer iterations than columns
        for (size_t i = result.iterationTimes.size() + 3; i < columnWidths.size(); ++i) {
            std::cout << std::setw(columnWidths[i]) << "-";
        }
    }
    std::cout << std::endl;
    
    // Print error message if failed
    if (result.failed && !result.errorMessage.empty()) {
        std::cout << std::setw(columnWidths[0]) << ""
                  << "Error: " << result.errorMessage << std::endl;
    }
}

// Specialization for ReaderResult
template<>
void ProgressiveTablePrinter<ReaderResult>::printRow(const ReaderResult& result) {
    std::cout << std::left
              << std::setw(columnWidths[0]) << result.label;
    
    if (result.failed) {
        std::cout << std::setw(columnWidths[1]) << "FAILED"
                  << std::setw(columnWidths[2]) << "-"
                  << std::setw(columnWidths[3]) << "-";
        // Fill remaining columns with "-" if there are iteration columns
        for (size_t i = 4; i < columnWidths.size(); ++i) {
            std::cout << std::setw(columnWidths[i]) << "-";
        }
    } else {
        std::cout << std::setw(columnWidths[1]) << result.cold;
        if (result.warmAvg > 0) {
            std::cout << std::setw(columnWidths[2]) << result.warmAvg
                      << std::setw(columnWidths[3]) << result.warmStddev;
        } else {
            std::cout << std::setw(columnWidths[2]) << "-"
                      << std::setw(columnWidths[3]) << "-";
        }
        
        // Print individual iteration times if available
        // For reader results, we'll display cold times first
        for (size_t i = 0; i < result.coldTimes.size() && i + 4 < columnWidths.size(); ++i) {
            std::cout << std::setw(columnWidths[i + 4]) << result.coldTimes[i];
        }
        
        // Fill remaining columns with "-" if we have fewer iterations than columns
        for (size_t i = result.coldTimes.size() + 4; i < columnWidths.size(); ++i) {
            std::cout << std::setw(columnWidths[i]) << "-";
        }
    }
    std::cout << std::endl;
    
    // Print error message if failed
    if (result.failed && !result.errorMessage.empty()) {
        std::cout << std::setw(columnWidths[0]) << ""
                  << "Error: " << result.errorMessage << std::endl;
    }
}