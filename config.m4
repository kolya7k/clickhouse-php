PHP_ARG_ENABLE([clickhouse],
	[whether to enable ClickHouse support],
	[AS_HELP_STRING([--enable-clickhouse],
		[Enable ClickHouse support])],
	[no]
)

if test "$PHP_CLICKHOUSE" != "no"; then
	PHP_REQUIRE_CXX()
	PHP_SUBST(CLICKHOUSE_SHARED_LIBADD)
	PHP_ADD_LIBRARY(stdc++, 1, CLICKHOUSE_SHARED_LIBADD)

 	CXXFLAGS="-fPIC -mno-sse4.2 -mno-sse4.1 -O2 -g3 -std=gnu++20 -Wall -Wextra -Wdeprecated -Wno-unused-parameter -Wredundant-decls -Wlogical-op -Wtrampolines -Wduplicated-cond -Wsuggest-override -Wdouble-promotion -Wno-unknown-pragmas -Wcast-qual -pedantic -pedantic-errors -fno-omit-frame-pointer -include defines.h"
	LDFLAGS="-fPIC -mno-sse4.2 -mno-sse4.1 -O2 -g3 -std=gnu++20 -Wl,--export-dynamic -fno-omit-frame-pointer"

	sources="clickhouse.cpp
		src/util.cpp \
		src/ClickHouse.cpp \
		src/ClickHouseResult.cpp \
		clickhouse-cpp/clickhouse/block.cpp \
		clickhouse-cpp/clickhouse/client.cpp \
		clickhouse-cpp/clickhouse/query.cpp \
		clickhouse-cpp/clickhouse/base/compressed.cpp \
		clickhouse-cpp/clickhouse/base/input.cpp \
		clickhouse-cpp/clickhouse/base/output.cpp \
		clickhouse-cpp/clickhouse/base/platform.cpp \
		clickhouse-cpp/clickhouse/base/socket.cpp \
		clickhouse-cpp/clickhouse/base/wire_format.cpp \
		clickhouse-cpp/clickhouse/columns/array.cpp \
		clickhouse-cpp/clickhouse/columns/date.cpp \
		clickhouse-cpp/clickhouse/columns/decimal.cpp \
		clickhouse-cpp/clickhouse/columns/enum.cpp \
		clickhouse-cpp/clickhouse/columns/factory.cpp \
		clickhouse-cpp/clickhouse/columns/ip4.cpp \
		clickhouse-cpp/clickhouse/columns/ip6.cpp \
		clickhouse-cpp/clickhouse/columns/itemview.cpp \
		clickhouse-cpp/clickhouse/columns/lowcardinality.cpp \
		clickhouse-cpp/clickhouse/columns/nullable.cpp \
		clickhouse-cpp/clickhouse/columns/numeric.cpp \
		clickhouse-cpp/clickhouse/columns/string.cpp \
		clickhouse-cpp/clickhouse/columns/tuple.cpp \
		clickhouse-cpp/clickhouse/columns/uuid.cpp \
		clickhouse-cpp/clickhouse/types/type_parser.cpp \
		clickhouse-cpp/clickhouse/types/types.cpp \
		clickhouse-cpp/contrib/cityhash/city.cc \
		clickhouse-cpp/contrib/lz4/lz4.c \
		clickhouse-cpp/contrib/lz4/lz4hc.c"

	PHP_ADD_INCLUDE(src)
	PHP_ADD_INCLUDE(clickhouse-cpp)
	PHP_ADD_INCLUDE(clickhouse-cpp/contrib)

	PHP_NEW_EXTENSION(clickhouse, $sources, $ext_shared,, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1)
fi