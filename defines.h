#pragma once

#define DATE_FORMAT		"%Y-%m-%d"
#define DATETIME_FORMAT		"%Y-%m-%d %H:%M:%S"

#include <cinttypes>
#include <cstring>
#include <ctime>
#include <cstdlib>

#include <string>
#include <string_view>
#include <deque>
#include <vector>
#include <unordered_map>
#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <clickhouse/client.h>
#pragma GCC diagnostic pop

using std::string;
using std::string_view;
using std::deque;
using std::vector;
using std::unordered_map;

using std::shared_ptr;
using std::make_shared;

using std::pair;

using namespace clickhouse;

#include "util.h"
#include "php_clickhouse.h"