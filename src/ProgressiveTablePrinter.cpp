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
    } else {
        std::cout << std::setw(columnWidths[1]) << result.avg
                  << std::setw(columnWidths[2]) << result.stddev;
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
    } else {
        std::cout << std::setw(columnWidths[1]) << result.cold;
        if (result.warmAvg > 0) {
            std::cout << std::setw(columnWidths[2]) << result.warmAvg
                      << std::setw(columnWidths[3]) << result.warmStddev;
        } else {
            std::cout << std::setw(columnWidths[2]) << "-"
                      << std::setw(columnWidths[3]) << "-";
        }
    }
    std::cout << std::endl;
    
    // Print error message if failed
    if (result.failed && !result.errorMessage.empty()) {
        std::cout << std::setw(columnWidths[0]) << ""
                  << "Error: " << result.errorMessage << std::endl;
    }
}