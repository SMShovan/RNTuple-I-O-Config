#pragma once
#include <vector>
#include "WriterResult.hpp"
#include "ReaderResult.hpp"
#include <map>
#include <utility>

void visualize_aos_writer_results(const std::vector<WriterResult>& results);
void visualize_aos_reader_results(const std::vector<ReaderResult>& results);
void visualize_aos_file_sizes(const std::vector<std::pair<std::string, double>>& sizes);
void visualize_aos_scaling(const std::map<std::string, std::vector<std::pair<int, double>>>& data); 