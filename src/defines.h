#pragma once

#define DATE_FORMAT		"%Y-%m-%d"
#define PHP_DATE_FORMAT		"Y-m-d"
#define PHP_DATETIME_FORMAT	"Y-m-d H:i:s"

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
#include <optional>
#include <limits>
#include <cmath>
#include <tuple>

using std::string;
using std::string_view;
using std::deque;
using std::vector;
using std::unordered_map;
using std::unordered_set;

using std::shared_ptr;
using std::make_shared;

using std::pair;

#include "clickhouse_php.h"

#include "clickhouse/client.h"

using clickhouse::Block;
using clickhouse::Client;
using clickhouse::ClientOptions;
using clickhouse::CompressionMethod;
using clickhouse::Query;
using clickhouse::ServerException;
using clickhouse::ValidationError;

using clickhouse::Bignum;
using clickhouse::Int128;
using clickhouse::UInt128;
using clickhouse::UUID;

using clickhouse::Type;
using clickhouse::TypeRef;
using clickhouse::ArrayType;
using clickhouse::DateTime64Type;
using clickhouse::DecimalType;
using clickhouse::EnumType;
using clickhouse::LowCardinalityType;
using clickhouse::MapType;
using clickhouse::NullableType;
using clickhouse::Time64Type;

using clickhouse::ColumnRef;
using clickhouse::ItemView;
using clickhouse::ColumnArray;
using clickhouse::ColumnBool;
using clickhouse::ColumnDate;
using clickhouse::ColumnDate32;
using clickhouse::ColumnDateTime;
using clickhouse::ColumnDateTime64;
using clickhouse::ColumnDecimal;
using clickhouse::ColumnEnum8;
using clickhouse::ColumnEnum16;
using clickhouse::ColumnFixedString;
using clickhouse::ColumnFloat32;
using clickhouse::ColumnFloat64;
using clickhouse::ColumnInt8;
using clickhouse::ColumnInt16;
using clickhouse::ColumnInt32;
using clickhouse::ColumnInt64;
using clickhouse::ColumnInt128;
using clickhouse::ColumnIPv4;
using clickhouse::ColumnIPv6;
using clickhouse::ColumnLowCardinality;
using clickhouse::ColumnLowCardinalityT;
using clickhouse::ColumnMap;
using clickhouse::ColumnMultiPolygon;
using clickhouse::ColumnNullable;
using clickhouse::ColumnPoint;
using clickhouse::ColumnPolygon;
using clickhouse::ColumnRing;
using clickhouse::ColumnString;
using clickhouse::ColumnTime;
using clickhouse::ColumnTime64;
using clickhouse::ColumnTuple;
using clickhouse::ColumnUInt8;
using clickhouse::ColumnUInt16;
using clickhouse::ColumnUInt32;
using clickhouse::ColumnUInt64;
using clickhouse::ColumnUInt128;
using clickhouse::ColumnUUID;