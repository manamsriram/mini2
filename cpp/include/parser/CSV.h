#ifndef CSV_H
#define CSV_H

#include <string>
#include <vector>

class CSV {
public:
    int year;
    int persons_injured;
    int persons_killed;
    int pedestrians_injured;
    int pedestrians_killed;
    int cyclists_injured;
    int cyclists_killed;
    int motorists_injured;
    int motorists_killed;
    int collision_id;
    std::string zip_code;
    std::string crash_date;
    std::string crash_time;
    std::string borough;
};

#endif 