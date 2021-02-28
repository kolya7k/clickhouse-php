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
	PHP_ADD_LIBRARY(clickhouse-cpp-lib, 1, CLICKHOUSE_SHARED_LIBADD)

 	CXXFLAGS="-fPIC -mno-sse4.2 -mno-sse4.1 -O2 -g3 -std=c++2a -Wall -Wextra -Wdeprecated -Wno-unused-parameter -Wredundant-decls -Wlogical-op -Wtrampolines -Wduplicated-cond -Wsuggest-override -Wdouble-promotion -Wno-unknown-pragmas -Wcast-qual -pedantic -pedantic-errors -fno-omit-frame-pointer -include defines.h -Isrc"
	LDFLAGS="-fPIC -mno-sse4.2 -mno-sse4.1 -O2 -g3 -Wl,--export-dynamic -fno-omit-frame-pointer"

	PHP_NEW_EXTENSION(clickhouse, clickhouse.cpp src/util.cpp src/ClickHouse.cpp src/ClickHouseResult.cpp, $ext_shared,, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1)
fi