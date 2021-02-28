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
#include <type_traits>
#include <memory>

using std::string;
using std::string_view;
using std::deque;
using std::vector;
using std::unordered_map;

using std::shared_ptr;
using std::make_shared;

using std::pair;

#include "php_clickhouse.h"

#include "clickhouse/client.h"

using namespace clickhouse;