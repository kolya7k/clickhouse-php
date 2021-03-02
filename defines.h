#pragma once

#define DATE_FORMAT		"%Y-%m-%d"
#define DATETIME_FORMAT		"%Y-%m-%d %H:%M:%S"

#define WITH_INT128		0

#include <cinttypes>
#include <cstring>
#include <ctime>
#include <cstdlib>

#include <string>
#include <string_view>
#include <deque>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <type_traits>
#include <memory>

using std::string;
using std::string_view;
using std::deque;
using std::vector;
using std::unordered_map;
using std::unordered_set;

using std::shared_ptr;
using std::make_shared;

using std::pair;

#include "php_clickhouse.h"

#include "clickhouse/client.h"

using namespace clickhouse;