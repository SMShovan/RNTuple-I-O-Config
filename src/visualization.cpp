#include <TH1F.h>
#include <TCanvas.h>
#include <TLegend.h>
#include <filesystem>
#include <vector>
#include <string>
#include <memory>
#include <iomanip>
#include <iostream>
#include <TText.h>
#include <map>
#include <vector>
#include <utility> // for std::pair
#include <TCanvas.h>
#include <TGraph.h>
#include <TString.h>

#include "ReaderResult.hpp"

#include "WriterResult.hpp"

using namespace std;

void visualize_reader_results(const vector<ReaderResult>& results) {
    filesystem::create_directory("../experiments");

    // Cold times bar plot
    auto hCold = make_unique<TH1F>("hCold", "Cold Read Times;Reader;Time (ms)", results.size(), 0, results.size());
    hCold->SetFillColor(kAzure - 2); // Muted blue color
    hCold->SetStats(0); // Remove stats box
    hCold->SetBarWidth(0.8); // Narrower bars
    hCold->SetBarOffset(0.1); // Space between bars
    for (size_t i = 0; i < results.size(); ++i) {
        hCold->SetBinContent(i + 1, results[i].cold);
        hCold->GetXaxis()->SetBinLabel(i + 1, results[i].label.c_str());
    }
    hCold->GetXaxis()->SetLabelSize(0.03);
    hCold->GetXaxis()->LabelsOption("v"); // Rotate labels vertically
    auto cCold = make_unique<TCanvas>("cCold", "Cold Read Times", 1600, 800);
    cCold->SetBottomMargin(0.3); // Increase bottom margin for labels
    hCold->Draw("BAR");
    // Add labels on top of bars
    double maxYCold = hCold->GetMaximum();
    double offsetCold = maxYCold * 0.05 + 1;
    for (int i = 1; i <= hCold->GetNbinsX(); ++i) {
        double yPos = hCold->GetBinContent(i) + offsetCold;
        auto text = make_unique<TText>(hCold->GetBinCenter(i), yPos, Form("%.1f", hCold->GetBinContent(i)));
        text->SetTextAlign(22); // Center
        text->SetTextSize(0.02);
        text->Draw();
    }
    cCold->SaveAs("../experiments/cold_times.pdf");

    // Warm times bar plot with error bars
    auto hWarm = make_unique<TH1F>("hWarm", "Warm Read Times;Reader;Time (ms)", results.size(), 0, results.size());
    hWarm->SetFillColor(kAzure - 2); // Muted blue color
    hWarm->SetStats(0);
    hWarm->SetBarWidth(0.8); // Narrower bars
    hWarm->SetBarOffset(0.1); // Space between bars
    for (size_t i = 0; i < results.size(); ++i) {
        hWarm->SetBinContent(i + 1, results[i].warmAvg);
        hWarm->SetBinError(i + 1, results[i].warmStddev);
        hWarm->GetXaxis()->SetBinLabel(i + 1, results[i].label.c_str());
    }
    hWarm->GetXaxis()->SetLabelSize(0.03);
    hWarm->GetXaxis()->LabelsOption("v");
    auto cWarm = make_unique<TCanvas>("cWarm", "Warm Read Times", 1600, 800);
    cWarm->SetBottomMargin(0.3);
    hWarm->Draw("BAR E");
    // Add labels on top of bars
    double maxYWarm = hWarm->GetMaximum();
    double offsetWarm = maxYWarm * 0.05 + 1;
    for (int i = 1; i <= hWarm->GetNbinsX(); ++i) {
        double yPos = hWarm->GetBinContent(i) + hWarm->GetBinError(i) + offsetWarm;
        auto text = make_unique<TText>(hWarm->GetBinCenter(i), yPos, Form("%.1f", hWarm->GetBinContent(i)));
        text->SetTextAlign(22); // Center
        text->SetTextSize(0.02);
        text->Draw();
    }
    cWarm->SaveAs("../experiments/warm_times.pdf");
}

void visualize_writer_results(const std::vector<WriterResult>& results) {
    std::cout << "Starting visualize_writer_results..." << std::endl;
    std::filesystem::create_directory("../experiments");

    // Write times bar plot with error bars
    auto hWrite = std::make_unique<TH1F>("hWrite", "Write Times;Writer;Time (ms)", results.size(), 0, results.size());
    hWrite->SetFillColor(kAzure - 2); // Muted blue color
    hWrite->SetStats(0);
    hWrite->SetBarWidth(0.8);
    hWrite->SetBarOffset(0.1);
    for (size_t i = 0; i < results.size(); ++i) {
        hWrite->SetBinContent(i + 1, results[i].avg);
        hWrite->SetBinError(i + 1, results[i].stddev);
        hWrite->GetXaxis()->SetBinLabel(i + 1, results[i].label.c_str());
    }
    hWrite->GetXaxis()->SetLabelSize(0.03);
    hWrite->GetXaxis()->LabelsOption("v");
    auto cWrite = std::make_unique<TCanvas>("cWrite", "Write Times", 1600, 800);
    cWrite->SetBottomMargin(0.3);
    hWrite->Draw("BAR E");
    // Adjust text position
    double maxY = hWrite->GetMaximum();
    double offset = maxY * 0.05 + 1; // 5% of max + 1
    for (int i = 1; i <= hWrite->GetNbinsX(); ++i) {
        double yPos = hWrite->GetBinContent(i) + hWrite->GetBinError(i) + offset;
        auto text = std::make_unique<TText>(hWrite->GetBinCenter(i), yPos, Form("%.1f", hWrite->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cWrite->SaveAs("../experiments/write_times.pdf");
    std::cout << "Write times plot saved to ../experiments/write_times.pdf" << std::endl;
}

void visualize_file_sizes(const std::vector<std::pair<std::string, double>>& fileSizes) {
    std::filesystem::create_directory("../experiments");

    auto hSize = std::make_unique<TH1F>("hSize", "File Sizes;Writer;Size (MB)", fileSizes.size(), 0, fileSizes.size());
    hSize->SetFillColor(kAzure - 2);
    hSize->SetStats(0);
    hSize->SetBarWidth(0.8);
    hSize->SetBarOffset(0.1);
    for (size_t i = 0; i < fileSizes.size(); ++i) {
        hSize->SetBinContent(i + 1, fileSizes[i].second);
        hSize->GetXaxis()->SetBinLabel(i + 1, fileSizes[i].first.c_str());
    }
    hSize->GetXaxis()->SetLabelSize(0.03);
    hSize->GetXaxis()->LabelsOption("v");
    auto cSize = std::make_unique<TCanvas>("cSize", "File Sizes", 1600, 800);
    cSize->SetBottomMargin(0.3);
    hSize->Draw("BAR");
    double maxY = hSize->GetMaximum();
    double offset = maxY * 0.05 + 1;
    for (int i = 1; i <= hSize->GetNbinsX(); ++i) {
        double yPos = hSize->GetBinContent(i) + offset;
        auto text = std::make_unique<TText>(hSize->GetBinCenter(i), yPos, Form("%.1f", hSize->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cSize->SaveAs("../experiments/file_sizes.pdf");
    std::cout << "File sizes plot saved to ../experiments/file_sizes.pdf" << std::endl;
}

void visualize_scaling(const std::map<std::string, std::vector<std::pair<int, double>>>& data) {
    if (data.empty()) return;

    int n = data.size();
    int cols = 3;
    int rows = (n + cols - 1) / cols; // Ceiling division

    TCanvas* canvas = new TCanvas("scaling_canvas", "Write Performance Scaling by Thread Count", 1800, 1200);
    canvas->Divide(cols, rows);

    int padIndex = 1;
    for (const auto& entry : data) {
        const auto& label = entry.first;
        const auto& points = entry.second;

        canvas->cd(padIndex++);
        gPad->SetLogx(1);
        TGraph* graph = new TGraph(points.size());
        for (size_t i = 0; i < points.size(); ++i) {
            graph->SetPoint(i, points[i].first, points[i].second);
        }
        graph->SetTitle(label.c_str());
        graph->GetXaxis()->SetTitle("Number of Threads");
        graph->GetYaxis()->SetTitle("Average Time (ms)");
        graph->Draw("ALP");
        graph->SetMarkerStyle(20);
        graph->SetLineWidth(2);
    }

    canvas->SaveAs("../experiments/scaling_plot.pdf");
    delete canvas;
} 