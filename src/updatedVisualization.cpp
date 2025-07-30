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
#include <TMultiGraph.h>
#include <TAxis.h>

#include "WriterResult.hpp"
#include "ReaderResult.hpp"

using namespace std;

void visualize_aos_writer_results(const std::vector<WriterResult>& results) {
    filesystem::create_directory("../experiments");

    // Write times bar plot with error bars
    auto hWrite = make_unique<TH1F>("hWriteAOS", "AOS Write Times;Writer;Time (ms)", results.size(), 0, results.size());
    hWrite->SetFillColor(kRed - 2); // Red color for AOS
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
    auto cWrite = make_unique<TCanvas>("cWriteAOS", "AOS Write Times", 1600, 800);
    cWrite->SetBottomMargin(0.3);
    hWrite->Draw("BAR E");
    // Add labels on top of bars
    double maxY = hWrite->GetMaximum();
    double offset = maxY * 0.05 + 1;
    for (int i = 1; i <= hWrite->GetNbinsX(); ++i) {
        double yPos = hWrite->GetBinContent(i) + hWrite->GetBinError(i) + offset;
        auto text = make_unique<TText>(hWrite->GetBinCenter(i), yPos, Form("%.1f", hWrite->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cWrite->SaveAs("../experiments/aos_write_times.pdf");
    cout << "AOS Write times plot saved to ../experiments/aos_write_times.pdf" << endl;
}

void visualize_aos_reader_results(const std::vector<ReaderResult>& results) {
    filesystem::create_directory("../experiments");

    // Cold times bar plot
    auto hCold = make_unique<TH1F>("hColdAOS", "AOS Cold Read Times;Reader;Time (ms)", results.size(), 0, results.size());
    hCold->SetFillColor(kRed - 2); // Red color for AOS
    hCold->SetStats(0);
    hCold->SetBarWidth(0.8);
    hCold->SetBarOffset(0.1);
    for (size_t i = 0; i < results.size(); ++i) {
        hCold->SetBinContent(i + 1, results[i].cold);
        hCold->GetXaxis()->SetBinLabel(i + 1, results[i].label.c_str());
    }
    hCold->GetXaxis()->SetLabelSize(0.03);
    hCold->GetXaxis()->LabelsOption("v");
    auto cCold = make_unique<TCanvas>("cColdAOS", "AOS Cold Read Times", 1600, 800);
    cCold->SetBottomMargin(0.3);
    hCold->Draw("BAR");
    // Add labels on top of bars
    double maxYCold = hCold->GetMaximum();
    double offsetCold = maxYCold * 0.05 + 1;
    for (int i = 1; i <= hCold->GetNbinsX(); ++i) {
        double yPos = hCold->GetBinContent(i) + offsetCold;
        auto text = make_unique<TText>(hCold->GetBinCenter(i), yPos, Form("%.1f", hCold->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cCold->SaveAs("../experiments/aos_cold_times.pdf");

    // Warm times bar plot with error bars
    auto hWarm = make_unique<TH1F>("hWarmAOS", "AOS Warm Read Times;Reader;Time (ms)", results.size(), 0, results.size());
    hWarm->SetFillColor(kRed - 2); // Red color for AOS
    hWarm->SetStats(0);
    hWarm->SetBarWidth(0.8);
    hWarm->SetBarOffset(0.1);
    for (size_t i = 0; i < results.size(); ++i) {
        hWarm->SetBinContent(i + 1, results[i].warmAvg);
        hWarm->SetBinError(i + 1, results[i].warmStddev);
        hWarm->GetXaxis()->SetBinLabel(i + 1, results[i].label.c_str());
    }
    hWarm->GetXaxis()->SetLabelSize(0.03);
    hWarm->GetXaxis()->LabelsOption("v");
    auto cWarm = make_unique<TCanvas>("cWarmAOS", "AOS Warm Read Times", 1600, 800);
    cWarm->SetBottomMargin(0.3);
    hWarm->Draw("BAR E");
    // Add labels on top of bars
    double maxYWarm = hWarm->GetMaximum();
    double offsetWarm = maxYWarm * 0.05 + 1;
    for (int i = 1; i <= hWarm->GetNbinsX(); ++i) {
        double yPos = hWarm->GetBinContent(i) + hWarm->GetBinError(i) + offsetWarm;
        auto text = make_unique<TText>(hWarm->GetBinCenter(i), yPos, Form("%.1f", hWarm->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cWarm->SaveAs("../experiments/aos_warm_times.pdf");
    cout << "AOS Read times plots saved to ../experiments/aos_cold_times.pdf and ../experiments/aos_warm_times.pdf" << endl;
}

void visualize_aos_file_sizes(const std::vector<std::pair<std::string, double>>& fileSizes) {
    filesystem::create_directory("../experiments");

    auto hSize = make_unique<TH1F>("hSizeAOS", "AOS File Sizes;Writer;Size (MB)", fileSizes.size(), 0, fileSizes.size());
    hSize->SetFillColor(kRed - 2); // Red color for AOS
    hSize->SetStats(0);
    hSize->SetBarWidth(0.8);
    hSize->SetBarOffset(0.1);
    for (size_t i = 0; i < fileSizes.size(); ++i) {
        hSize->SetBinContent(i + 1, fileSizes[i].second);
        hSize->GetXaxis()->SetBinLabel(i + 1, fileSizes[i].first.c_str());
    }
    hSize->GetXaxis()->SetLabelSize(0.03);
    hSize->GetXaxis()->LabelsOption("v");
    auto cSize = make_unique<TCanvas>("cSizeAOS", "AOS File Sizes", 1600, 800);
    cSize->SetBottomMargin(0.3);
    hSize->Draw("BAR");
    double maxY = hSize->GetMaximum();
    double offset = maxY * 0.05 + 1;
    for (int i = 1; i <= hSize->GetNbinsX(); ++i) {
        double yPos = hSize->GetBinContent(i) + offset;
        auto text = make_unique<TText>(hSize->GetBinCenter(i), yPos, Form("%.1f", hSize->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cSize->SaveAs("../experiments/aos_file_sizes.pdf");
    cout << "AOS File sizes plot saved to ../experiments/aos_file_sizes.pdf" << endl;
}

void visualize_aos_scaling(const std::map<std::string, std::vector<std::pair<int, double>>>& data) {
    if (data.empty()) return;

    filesystem::create_directory("../experiments");

    int n = data.size();
    int cols = 3;
    int rows = (n + cols - 1) / cols; // Ceiling division

    TCanvas* canvas = new TCanvas("aos_scaling_canvas", "AOS Write Performance Scaling by Thread Count", 1800, 1200);
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
        graph->SetTitle(("AOS " + label).c_str());
        graph->GetXaxis()->SetTitle("Number of Threads");
        graph->GetYaxis()->SetTitle("Average Time (ms)");
        graph->Draw("ALP");
        graph->SetMarkerStyle(20);
        graph->SetLineWidth(2);
        graph->SetLineColor(kRed);
        graph->SetMarkerColor(kRed);
    }

    canvas->SaveAs("../experiments/aos_scaling_plot.pdf");
    delete canvas;
    cout << "AOS Scaling plot saved to ../experiments/aos_scaling_plot.pdf" << endl;
}

// SOA visualization functions
void visualize_soa_writer_results(const std::vector<WriterResult>& results) {
    filesystem::create_directory("../experiments");

    // Write times bar plot with error bars
    auto hWrite = make_unique<TH1F>("hWriteSOA", "SOA Write Times;Writer;Time (ms)", results.size(), 0, results.size());
    hWrite->SetFillColor(kBlue - 2); // Blue color for SOA
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
    auto cWrite = make_unique<TCanvas>("cWriteSOA", "SOA Write Times", 1600, 800);
    cWrite->SetBottomMargin(0.3);
    hWrite->Draw("BAR E");
    // Add labels on top of bars
    double maxY = hWrite->GetMaximum();
    double offset = maxY * 0.05 + 1;
    for (int i = 1; i <= hWrite->GetNbinsX(); ++i) {
        double yPos = hWrite->GetBinContent(i) + hWrite->GetBinError(i) + offset;
        auto text = make_unique<TText>(hWrite->GetBinCenter(i), yPos, Form("%.1f", hWrite->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cWrite->SaveAs("../experiments/soa_write_times.pdf");
    cout << "SOA Write times plot saved to ../experiments/soa_write_times.pdf" << endl;
}

void visualize_soa_reader_results(const std::vector<ReaderResult>& results) {
    filesystem::create_directory("../experiments");

    // Cold times bar plot
    auto hCold = make_unique<TH1F>("hColdSOA", "SOA Cold Read Times;Reader;Time (ms)", results.size(), 0, results.size());
    hCold->SetFillColor(kBlue - 2); // Blue color for SOA
    hCold->SetStats(0);
    hCold->SetBarWidth(0.8);
    hCold->SetBarOffset(0.1);
    for (size_t i = 0; i < results.size(); ++i) {
        hCold->SetBinContent(i + 1, results[i].cold);
        hCold->GetXaxis()->SetBinLabel(i + 1, results[i].label.c_str());
    }
    hCold->GetXaxis()->SetLabelSize(0.03);
    hCold->GetXaxis()->LabelsOption("v");
    auto cCold = make_unique<TCanvas>("cColdSOA", "SOA Cold Read Times", 1600, 800);
    cCold->SetBottomMargin(0.3);
    hCold->Draw("BAR");
    // Add labels on top of bars
    double maxYCold = hCold->GetMaximum();
    double offsetCold = maxYCold * 0.05 + 1;
    for (int i = 1; i <= hCold->GetNbinsX(); ++i) {
        double yPos = hCold->GetBinContent(i) + offsetCold;
        auto text = make_unique<TText>(hCold->GetBinCenter(i), yPos, Form("%.1f", hCold->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cCold->SaveAs("../experiments/soa_cold_times.pdf");

    // Warm times bar plot with error bars
    auto hWarm = make_unique<TH1F>("hWarmSOA", "SOA Warm Read Times;Reader;Time (ms)", results.size(), 0, results.size());
    hWarm->SetFillColor(kBlue - 2); // Blue color for SOA
    hWarm->SetStats(0);
    hWarm->SetBarWidth(0.8);
    hWarm->SetBarOffset(0.1);
    for (size_t i = 0; i < results.size(); ++i) {
        hWarm->SetBinContent(i + 1, results[i].warmAvg);
        hWarm->SetBinError(i + 1, results[i].warmStddev);
        hWarm->GetXaxis()->SetBinLabel(i + 1, results[i].label.c_str());
    }
    hWarm->GetXaxis()->SetLabelSize(0.03);
    hWarm->GetXaxis()->LabelsOption("v");
    auto cWarm = make_unique<TCanvas>("cWarmSOA", "SOA Warm Read Times", 1600, 800);
    cWarm->SetBottomMargin(0.3);
    hWarm->Draw("BAR E");
    // Add labels on top of bars
    double maxYWarm = hWarm->GetMaximum();
    double offsetWarm = maxYWarm * 0.05 + 1;
    for (int i = 1; i <= hWarm->GetNbinsX(); ++i) {
        double yPos = hWarm->GetBinContent(i) + hWarm->GetBinError(i) + offsetWarm;
        auto text = make_unique<TText>(hWarm->GetBinCenter(i), yPos, Form("%.1f", hWarm->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cWarm->SaveAs("../experiments/soa_warm_times.pdf");
    cout << "SOA Read times plots saved to ../experiments/soa_cold_times.pdf and ../experiments/soa_warm_times.pdf" << endl;
}

void visualize_soa_file_sizes(const std::vector<std::pair<std::string, double>>& fileSizes) {
    filesystem::create_directory("../experiments");

    auto hSize = make_unique<TH1F>("hSizeSOA", "SOA File Sizes;Writer;Size (MB)", fileSizes.size(), 0, fileSizes.size());
    hSize->SetFillColor(kBlue - 2); // Blue color for SOA
    hSize->SetStats(0);
    hSize->SetBarWidth(0.8);
    hSize->SetBarOffset(0.1);
    for (size_t i = 0; i < fileSizes.size(); ++i) {
        hSize->SetBinContent(i + 1, fileSizes[i].second);
        hSize->GetXaxis()->SetBinLabel(i + 1, fileSizes[i].first.c_str());
    }
    hSize->GetXaxis()->SetLabelSize(0.03);
    hSize->GetXaxis()->LabelsOption("v");
    auto cSize = make_unique<TCanvas>("cSizeSOA", "SOA File Sizes", 1600, 800);
    cSize->SetBottomMargin(0.3);
    hSize->Draw("BAR");
    double maxY = hSize->GetMaximum();
    double offset = maxY * 0.05 + 1;
    for (int i = 1; i <= hSize->GetNbinsX(); ++i) {
        double yPos = hSize->GetBinContent(i) + offset;
        auto text = make_unique<TText>(hSize->GetBinCenter(i), yPos, Form("%.1f", hSize->GetBinContent(i)));
        text->SetTextAlign(22);
        text->SetTextSize(0.02);
        text->Draw();
    }
    cSize->SaveAs("../experiments/soa_file_sizes.pdf");
    cout << "SOA File sizes plot saved to ../experiments/soa_file_sizes.pdf" << endl;
}

void visualize_soa_scaling(const std::map<std::string, std::vector<std::pair<int, double>>>& data) {
    if (data.empty()) return;

    filesystem::create_directory("../experiments");

    int n = data.size();
    int cols = 3;
    int rows = (n + cols - 1) / cols; // Ceiling division

    TCanvas* canvas = new TCanvas("soa_scaling_canvas", "SOA Write Performance Scaling by Thread Count", 1800, 1200);
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
        graph->SetTitle(("SOA " + label).c_str());
        graph->GetXaxis()->SetTitle("Number of Threads");
        graph->GetYaxis()->SetTitle("Average Time (ms)");
        graph->Draw("ALP");
        graph->SetMarkerStyle(20);
        graph->SetLineWidth(2);
        graph->SetLineColor(kBlue);
        graph->SetMarkerColor(kBlue);
    }

    canvas->SaveAs("../experiments/soa_scaling_plot.pdf");
    delete canvas;
    cout << "SOA Scaling plot saved to ../experiments/soa_scaling_plot.pdf" << endl;
}

// Comparison functions for AOS vs SOA
void visualize_comparison_writer_results(const std::vector<WriterResult>& aosResults, const std::vector<WriterResult>& soaResults) {
    filesystem::create_directory("../experiments");

    // Create comparison plot
    auto hWrite = make_unique<TH1F>("hWriteComp", "AOS vs SOA Write Times;Writer;Time (ms)", aosResults.size(), 0, aosResults.size());
    hWrite->SetStats(0);
    hWrite->SetBarWidth(0.35);
    hWrite->SetBarOffset(0.1);
    
    // AOS data
    for (size_t i = 0; i < aosResults.size(); ++i) {
        hWrite->SetBinContent(i + 1, aosResults[i].avg);
        hWrite->SetBinError(i + 1, aosResults[i].stddev);
        hWrite->GetXaxis()->SetBinLabel(i + 1, aosResults[i].label.c_str());
    }
    hWrite->SetFillColor(kRed - 2);
    
    auto cWrite = make_unique<TCanvas>("cWriteComp", "AOS vs SOA Write Times", 1600, 800);
    cWrite->SetBottomMargin(0.3);
    hWrite->Draw("BAR E");
    
    // SOA data as overlay
    auto hWriteSOA = make_unique<TH1F>("hWriteSOAComp", "SOA Write Times", aosResults.size(), 0, aosResults.size());
    hWriteSOA->SetBarWidth(0.35);
    hWriteSOA->SetBarOffset(0.55);
    for (size_t i = 0; i < soaResults.size(); ++i) {
        hWriteSOA->SetBinContent(i + 1, soaResults[i].avg);
        hWriteSOA->SetBinError(i + 1, soaResults[i].stddev);
    }
    hWriteSOA->SetFillColor(kBlue - 2);
    hWriteSOA->Draw("BAR E SAME");
    
    // Add legend
    auto legend = make_unique<TLegend>(0.7, 0.8, 0.9, 0.9);
    legend->AddEntry(hWrite.get(), "AOS", "f");
    legend->AddEntry(hWriteSOA.get(), "SOA", "f");
    legend->Draw();
    
    cWrite->SaveAs("../experiments/comparison_write_times.pdf");
    cout << "Comparison write times plot saved to ../experiments/comparison_write_times.pdf" << endl;
}

void visualize_comparison_reader_results(const std::vector<ReaderResult>& aosResults, const std::vector<ReaderResult>& soaResults) {
    filesystem::create_directory("../experiments");

    // Cold times comparison
    auto hCold = make_unique<TH1F>("hColdComp", "AOS vs SOA Cold Read Times;Reader;Time (ms)", aosResults.size(), 0, aosResults.size());
    hCold->SetStats(0);
    hCold->SetBarWidth(0.35);
    hCold->SetBarOffset(0.1);
    
    for (size_t i = 0; i < aosResults.size(); ++i) {
        hCold->SetBinContent(i + 1, aosResults[i].cold);
        hCold->GetXaxis()->SetBinLabel(i + 1, aosResults[i].label.c_str());
    }
    hCold->SetFillColor(kRed - 2);
    
    auto cCold = make_unique<TCanvas>("cColdComp", "AOS vs SOA Cold Read Times", 1600, 800);
    cCold->SetBottomMargin(0.3);
    hCold->Draw("BAR");
    
    auto hColdSOA = make_unique<TH1F>("hColdSOAComp", "SOA Cold Read Times", aosResults.size(), 0, aosResults.size());
    hColdSOA->SetBarWidth(0.35);
    hColdSOA->SetBarOffset(0.55);
    for (size_t i = 0; i < soaResults.size(); ++i) {
        hColdSOA->SetBinContent(i + 1, soaResults[i].cold);
    }
    hColdSOA->SetFillColor(kBlue - 2);
    hColdSOA->Draw("BAR SAME");
    
    auto legendCold = make_unique<TLegend>(0.7, 0.8, 0.9, 0.9);
    legendCold->AddEntry(hCold.get(), "AOS", "f");
    legendCold->AddEntry(hColdSOA.get(), "SOA", "f");
    legendCold->Draw();
    
    cCold->SaveAs("../experiments/comparison_cold_times.pdf");

    // Warm times comparison
    auto hWarm = make_unique<TH1F>("hWarmComp", "AOS vs SOA Warm Read Times;Reader;Time (ms)", aosResults.size(), 0, aosResults.size());
    hWarm->SetStats(0);
    hWarm->SetBarWidth(0.35);
    hWarm->SetBarOffset(0.1);
    
    for (size_t i = 0; i < aosResults.size(); ++i) {
        hWarm->SetBinContent(i + 1, aosResults[i].warmAvg);
        hWarm->SetBinError(i + 1, aosResults[i].warmStddev);
        hWarm->GetXaxis()->SetBinLabel(i + 1, aosResults[i].label.c_str());
    }
    hWarm->SetFillColor(kRed - 2);
    
    auto cWarm = make_unique<TCanvas>("cWarmComp", "AOS vs SOA Warm Read Times", 1600, 800);
    cWarm->SetBottomMargin(0.3);
    hWarm->Draw("BAR E");
    
    auto hWarmSOA = make_unique<TH1F>("hWarmSOAComp", "SOA Warm Read Times", aosResults.size(), 0, aosResults.size());
    hWarmSOA->SetBarWidth(0.35);
    hWarmSOA->SetBarOffset(0.55);
    for (size_t i = 0; i < soaResults.size(); ++i) {
        hWarmSOA->SetBinContent(i + 1, soaResults[i].warmAvg);
        hWarmSOA->SetBinError(i + 1, soaResults[i].warmStddev);
    }
    hWarmSOA->SetFillColor(kBlue - 2);
    hWarmSOA->Draw("BAR E SAME");
    
    auto legendWarm = make_unique<TLegend>(0.7, 0.8, 0.9, 0.9);
    legendWarm->AddEntry(hWarm.get(), "AOS", "f");
    legendWarm->AddEntry(hWarmSOA.get(), "SOA", "f");
    legendWarm->Draw();
    
    cWarm->SaveAs("../experiments/comparison_warm_times.pdf");
    cout << "Comparison read times plots saved to ../experiments/comparison_cold_times.pdf and ../experiments/comparison_warm_times.pdf" << endl;
}

void visualize_comparison_file_sizes(const std::vector<std::pair<std::string, double>>& aosSizes, const std::vector<std::pair<std::string, double>>& soaSizes) {
    filesystem::create_directory("../experiments");

    auto hSize = make_unique<TH1F>("hSizeComp", "AOS vs SOA File Sizes;Writer;Size (MB)", aosSizes.size(), 0, aosSizes.size());
    hSize->SetStats(0);
    hSize->SetBarWidth(0.35);
    hSize->SetBarOffset(0.1);
    
    for (size_t i = 0; i < aosSizes.size(); ++i) {
        hSize->SetBinContent(i + 1, aosSizes[i].second);
        hSize->GetXaxis()->SetBinLabel(i + 1, aosSizes[i].first.c_str());
    }
    hSize->SetFillColor(kRed - 2);
    
    auto cSize = make_unique<TCanvas>("cSizeComp", "AOS vs SOA File Sizes", 1600, 800);
    cSize->SetBottomMargin(0.3);
    hSize->Draw("BAR");
    
    auto hSizeSOA = make_unique<TH1F>("hSizeSOAComp", "SOA File Sizes", aosSizes.size(), 0, aosSizes.size());
    hSizeSOA->SetBarWidth(0.35);
    hSizeSOA->SetBarOffset(0.55);
    for (size_t i = 0; i < soaSizes.size(); ++i) {
        hSizeSOA->SetBinContent(i + 1, soaSizes[i].second);
    }
    hSizeSOA->SetFillColor(kBlue - 2);
    hSizeSOA->Draw("BAR SAME");
    
    auto legend = make_unique<TLegend>(0.7, 0.8, 0.9, 0.9);
    legend->AddEntry(hSize.get(), "AOS", "f");
    legend->AddEntry(hSizeSOA.get(), "SOA", "f");
    legend->Draw();
    
    cSize->SaveAs("../experiments/comparison_file_sizes.pdf");
    cout << "Comparison file sizes plot saved to ../experiments/comparison_file_sizes.pdf" << endl;
}