#pragma once

#include <cinttypes>
#include <cstring>

#include <string>
#include <string_view>
#include <deque>
#include <vector>
#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <clickhouse/client.h>
#pragma GCC diagnostic pop

using std::string;
using std::string_view;
using std::deque;
using std::vector;

using std::shared_ptr;
using std::make_shared;

using namespace clickhouse;

#include "util.h"
#include "php_clickhouse.h"