#include "include/ProgressiveTablePrinter.hpp"
#include "include/WriterResult.hpp"
#include "include/ReaderResult.hpp"
#include <chrono>
#include <thread>
#include <iostream>
#include <stdexcept>

// Mock benchmark functions that simulate success and failure
double mock_successful_writer() {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    return 125.5; // Return time in ms
}

double mock_failing_writer() {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    throw std::runtime_error("File not found: output/missing_file.root");
}

double mock_successful_reader() {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    return 95.2; // Return time in ms
}

double mock_failing_reader() {
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    throw std::invalid_argument("Invalid ROOT file format");
}

int main() {
    std::cout << "Testing Error Handling in Progressive Tables...\n" << std::endl;
    
    // Test WriterResult table with some failures
    ProgressiveTablePrinter<WriterResult> writerTable(
        "Writer Benchmarks with Error Handling Demo",
        {"Writer", "Average (ms)", "StdDev (ms)"},
        {32, 16, 16}
    );
    
    auto writerBenchmark = [&](const std::string& label, auto func) {
        std::cout << "Running " << label << "..." << std::flush;
        WriterResult result = {label, 0.0, 0.0, false, ""};
        
        try {
            std::vector<double> times;
            for (int i = 0; i < 3; ++i) {
                double t = func();
                times.push_back(t);
            }
            double avg = 0.0;
            for (double t : times) avg += t;
            avg /= times.size();
            result.avg = avg;
            result.stddev = 5.0; // Mock stddev
            std::cout << " DONE" << std::endl;
        } catch (const std::exception& e) {
            result.failed = true;
            result.errorMessage = e.what();
            std::cout << " FAILED" << std::endl;
        } catch (...) {
            result.failed = true;
            result.errorMessage = "Unknown error occurred";
            std::cout << " FAILED" << std::endl;
        }
        
        writerTable.addRow(result);
    };
    
    writerBenchmark("AOS_event_allDataProduct", mock_successful_writer);
    writerBenchmark("AOS_event_perDataProduct", mock_failing_writer);
    writerBenchmark("SOA_event_allDataProduct", mock_successful_writer);
    writerBenchmark("SOA_event_perDataProduct", mock_failing_writer);
    writerBenchmark("AOS_spill_allDataProduct", mock_successful_writer);
    
    writerTable.printFooter();
    
    std::cout << std::endl;
    
    // Test ReaderResult table with some failures
    ProgressiveTablePrinter<ReaderResult> readerTable(
        "Reader Benchmarks with Error Handling Demo",
        {"Reader", "Cold (ms)", "Warm Avg (ms)", "Warm StdDev (ms)"},
        {32, 16, 16, 16}
    );
    
    auto readerBenchmark = [&](const std::string& label, auto func) {
        std::cout << "Running " << label << "..." << std::flush;
        ReaderResult result = {label, 0.0, 0.0, 0.0, false, ""};
        
        try {
            std::vector<double> coldTimes, warmTimes;
            for (int i = 0; i < 2; ++i) {
                double cold = func();
                coldTimes.push_back(cold);
                double warm = func(); // Second read for warm
                warmTimes.push_back(warm);
            }
            double coldAvg = 0.0, warmAvg = 0.0;
            for (double t : coldTimes) coldAvg += t;
            for (double t : warmTimes) warmAvg += t;
            coldAvg /= coldTimes.size();
            warmAvg /= warmTimes.size();
            result.cold = coldAvg;
            result.warmAvg = warmAvg;
            result.warmStddev = 3.0; // Mock stddev
            std::cout << " DONE" << std::endl;
        } catch (const std::exception& e) {
            result.failed = true;
            result.errorMessage = e.what();
            std::cout << " FAILED" << std::endl;
        } catch (...) {
            result.failed = true;
            result.errorMessage = "Unknown error occurred";
            std::cout << " FAILED" << std::endl;
        }
        
        readerTable.addRow(result);
    };
    
    readerBenchmark("AOS_event_allDataProduct", mock_successful_reader);
    readerBenchmark("AOS_event_perDataProduct", mock_successful_reader);
    readerBenchmark("SOA_event_allDataProduct", mock_failing_reader);
    readerBenchmark("SOA_event_perDataProduct", mock_successful_reader);
    readerBenchmark("AOS_spill_allDataProduct", mock_failing_reader);
    
    readerTable.printFooter();
    
    std::cout << "\nDemo completed! Notice how failed benchmarks are marked as 'FAILED' with error messages," << std::endl;
    std::cout << "while successful benchmarks continue to run normally." << std::endl;
    
    return 0;
}