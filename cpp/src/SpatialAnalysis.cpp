#include "./parser/CSV.h"
#include "../include/parser/SpatialAnalysis.h"
#include <iostream>
#include <algorithm>
#include "proto/mini2.grpc.pb.h"
#include "proto/mini2.pb.h"

SpatialAnalysis::SpatialAnalysis(int injuryThreshold, int deathThreshold)
    : INJURY_THRESHOLD(injuryThreshold), DEATH_THRESHOLD(deathThreshold) {}

    void SpatialAnalysis::processCollisions(const std::vector<mini2::CollisionData>& data) {
        for (const auto& row : data) {
            if (!row.borough().empty() && !row.zip_code().empty()) {
                int year = extractYear(row.crash_date());
                auto& areaStats = boroughZipStats[row.borough()][row.zip_code()];
                
                auto it = std::lower_bound(areaStats.yearlyStats.begin(), areaStats.yearlyStats.end(), year,
                    [](const YearlyStats& stats, int y) { return stats.year < y; });
                
                if (it == areaStats.yearlyStats.end() || it->year != year) {
                    it = areaStats.yearlyStats.insert(it, {year, 0, 0, 0});
                }
                
                it->collisionCount++;
                it->injuryCount += std::max(0, row.number_of_persons_injured());
                it->deathCount += std::max(0, row.number_of_persons_killed());
            }
        }
    }    
    
void SpatialAnalysis::identifyHighRiskAreas() const {
    std::cout << "Risk assessment by borough and zip code:\n";
    for (const auto& [borough, zipStats] : boroughZipStats) {
        for (const auto& [zipCode, areaStats] : zipStats) {
            auto assessment = assessRisk(areaStats.yearlyStats);
            if (assessment.isHighRisk) {
                std::cout << "Borough: " << borough << ", Zip Code: " << zipCode << " ";
                if (assessment.hasReducedRisk) {
                    std::cout << "(REDUCED RISK)\n";
                } else {
                    std::cout << "(HIGH RISK)\n";
                }
                for (const auto& yearStats : areaStats.yearlyStats) {
                    std::cout << "  Year " << yearStats.year << ": "
                              << yearStats.collisionCount << " collisions, "
                              << yearStats.injuryCount << " injuries, "
                              << yearStats.deathCount << " deaths\n";
                }
                std::cout << std::endl;
            }
        }
    }
}

int SpatialAnalysis::extractYear(const std::string& date) const {
    return std::stoi(date.substr(6, 4));
}

SpatialAnalysis::RiskAssessment SpatialAnalysis::assessRisk(const std::vector<YearlyStats>& stats) const {
    if (stats.empty()) return {false, false};

    bool everHighRisk = false;
    bool hasReducedRisk = false;
    int lastHighRiskYear = -1;

    for (size_t i = 0; i < stats.size(); ++i) {
        const auto& yearStat = stats[i];
        bool currentYearHighRisk = (yearStat.injuryCount >= INJURY_THRESHOLD || yearStat.deathCount >= DEATH_THRESHOLD);

        if (currentYearHighRisk) {
            everHighRisk = true;
            lastHighRiskYear = yearStat.year;
        } else if (everHighRisk && i > 0) {
            const auto& lastHighRiskStat = *std::find_if(stats.rbegin(), stats.rend(),
                [lastHighRiskYear](const YearlyStats& s) { return s.year == lastHighRiskYear; });
            
            if (yearStat.injuryCount < lastHighRiskStat.injuryCount && yearStat.deathCount < lastHighRiskStat.deathCount) {
                hasReducedRisk = true;
            }
        }
    }

    return {everHighRisk, hasReducedRisk};
}
