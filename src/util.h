#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"

namespace std
{
	string to_string(clickhouse::Int128 value);
}

#pragma GCC diagnostic pop