#include "include/ProgressiveTablePrinter.hpp"
#include "include/WriterResult.hpp"
#include "include/ReaderResult.hpp"
#include <chrono>
#include <thread>
#include <iostream>

int main() {
    std::cout << "Testing Progressive Table Output...\n" << std::endl;
    
    // Test WriterResult table with simulated benchmarks
    ProgressiveTablePrinter<WriterResult> writerTable(
        "Writer Benchmarks Demo (Progressive Results)",
        {"Writer", "Average (ms)", "StdDev (ms)"},
        {32, 16, 16}
    );
    
    std::vector<std::pair<std::string, std::pair<double, double>>> writerTests = {
        {"AOS_event_allDataProduct", {120.5, 5.2}},
        {"AOS_event_perDataProduct", {185.8, 8.1}},
        {"SOA_event_allDataProduct", {95.3, 6.7}}
    };
    
    for (const auto& test : writerTests) {
        std::cout << "Running " << test.first << "..." << std::flush;
        std::this_thread::sleep_for(std::chrono::milliseconds(800)); // Simulate benchmark work
        std::cout << " DONE" << std::endl;
        WriterResult result = {test.first, test.second.first, test.second.second};
        writerTable.addRow(result);
    }
    writerTable.printFooter();
    
    std::cout << std::endl;
    
    // Test ReaderResult table
    ProgressiveTablePrinter<ReaderResult> readerTable(
        "Reader Benchmarks Demo (Progressive Results)",
        {"Reader", "Cold (ms)", "Warm Avg (ms)", "Warm StdDev (ms)"},
        {32, 16, 16, 16}
    );
    
    std::vector<std::tuple<std::string, double, double, double>> readerTests = {
        {"AOS_event_allDataProduct", 140.3, 95.1, 3.4},
        {"AOS_event_perDataProduct", 210.7, 145.2, 5.8},
        {"SOA_event_allDataProduct", 115.9, 85.6, 4.1}
    };
    
    for (const auto& test : readerTests) {
        std::cout << "Running " << std::get<0>(test) << "..." << std::flush;
        std::this_thread::sleep_for(std::chrono::milliseconds(800)); // Simulate benchmark work
        std::cout << " DONE" << std::endl;
        ReaderResult result = {std::get<0>(test), std::get<1>(test), std::get<2>(test), std::get<3>(test)};
        readerTable.addRow(result);
    }
    readerTable.printFooter();
    
    std::cout << "\nDemo completed! This is how your benchmarks will now appear." << std::endl;
    return 0;
}