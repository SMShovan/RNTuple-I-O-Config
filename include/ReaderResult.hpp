#ifndef READERRESULT_HPP
#define READERRESULT_HPP

#include <string>
#include <vector>

struct ReaderResult {
    std::string label;
    double cold;
    double warmAvg;
    double warmStddev;
};

std::vector<ReaderResult> in(int nThreads, int iter);

#endif 