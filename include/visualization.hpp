#ifndef VISUALIZATION_HPP
#define VISUALIZATION_HPP

#include "ReaderResult.hpp"
#include <vector>

#include "WriterResult.hpp"
#include <map>
#include <vector>
#include <utility> // for std::pair

void visualize_reader_results(const std::vector<ReaderResult>& results);

void visualize_writer_results(const std::vector<WriterResult>& results);

void visualize_file_sizes(const std::vector<std::pair<std::string, double>>& fileSizes);

void visualize_scaling(const std::map<std::string, std::vector<std::pair<int, double>>>& data);

#endif 