PHP_ARG_ENABLE([clickhouse],
	[whether to enable ClickHouse support],
	[AS_HELP_STRING([--enable-clickhouse],
		[Enable ClickHouse support])],
	[no]
)

if test "$CLICKHOUSE" != "no"; then
	PHP_REQUIRE_CXX()
	PHP_SUBST(CLICKHOUSE_SHARED_LIBADD)
	PHP_ADD_LIBRARY(stdc++, 1, CLICKHOUSE_SHARED_LIBADD)
	PHP_ADD_LIBRARY(lz4, 1, CLICKHOUSE_SHARED_LIBADD)
	PHP_ADD_LIBRARY(zstd, 1, CLICKHOUSE_SHARED_LIBADD)

	system_includes="-isystem $phpincludedir -isystem $phpincludedir/main -isystem $phpincludedir/TSRM -isystem $phpincludedir/Zend -isystem $phpincludedir/ext -isystem $phpincludedir/ext/date/lib -isystem PHP_EXT_SRCDIR()/clickhouse-cpp -isystem PHP_EXT_SRCDIR()/clickhouse-cpp/clickhouse/cityhash"

	CXXFLAGS="-fPIC -march=native -m64 -pipe -O2 -g3 -fno-omit-frame-pointer -std=gnu++2a $system_includes"
	LDFLAGS="-fPIC -O2 -g3 -fno-omit-frame-pointer -Wl,--export-dynamic -Wl,-z,relro -Wl,-z,now -Wl,--warn-common"

	AC_LANG_PUSH([C++])
	AC_COMPILE_IFELSE([AC_LANG_PROGRAM([], [[
#ifndef __clang__
#error
#endif
	]])], [cxx_is_clang=yes], [cxx_is_clang=no])
	AC_LANG_POP([C++])

	extension_flags="-include src/defines.h -Wall -Wextra -Werror -Wdeprecated -Wconversion -Wredundant-decls -Wfloat-equal -Wcast-qual -Wdouble-promotion -Wmissing-include-dirs -Wundef -Wuninitialized -Wzero-as-null-pointer-constant -Wnon-virtual-dtor -Woverloaded-virtual -pedantic -pedantic-errors -Wno-unused-parameter"

	if test "$cxx_is_clang" = "yes"; then
		extension_flags="$extension_flags -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-sign-conversion -Wno-padded -Wno-global-constructors -Wno-reserved-identifier -Wno-exit-time-destructors -Wno-newline-eof -Wno-switch-enum -Wno-old-style-cast -Wno-cast-align -Wno-disabled-macro-expansion -Wno-unused-macros -Wno-class-varargs -Wno-format-nonliteral -Wno-shadow-field-in-constructor -Wno-ctad-maybe-unsupported -Wno-switch-default -Wno-missing-braces -Wno-unsafe-buffer-usage -Wno-nrvo -Wno-date-time -Wno-missing-prototypes -Wno-lifetime-safety-suggestions -Wno-lifetime-safety-invalidation"
	else
		extension_flags="$extension_flags -Wconditionally-supported -Wcast-align=strict -Wlogical-op -Wuseless-cast -Wtrampolines -Wduplicated-cond -Wsuggest-override -Wno-unknown-pragmas -Wformat=2 -Wnull-dereference -Wduplicated-branches -Wimplicit-fallthrough=5 -Warray-bounds=2 -Wstrict-overflow=2 -Wuse-after-free=3 -Wbidi-chars=any -Wdangling-reference"
	fi

	extension_sources="src/clickhouse.cpp \
		src/util.cpp \
		src/ClickHouseDB.cpp \
		src/ClickHouseResult.cpp"

	library_sources="clickhouse-cpp/clickhouse/block.cpp \
		clickhouse-cpp/clickhouse/client.cpp \
		clickhouse-cpp/clickhouse/query.cpp \
		clickhouse-cpp/clickhouse/base/compressed.cpp \
		clickhouse-cpp/clickhouse/base/endpoints_iterator.cpp \
		clickhouse-cpp/clickhouse/base/input.cpp \
		clickhouse-cpp/clickhouse/base/output.cpp \
		clickhouse-cpp/clickhouse/base/platform.cpp \
		clickhouse-cpp/clickhouse/base/socket.cpp \
		clickhouse-cpp/clickhouse/base/sslsocket.cpp \
		clickhouse-cpp/clickhouse/base/wire_format.cpp \
		clickhouse-cpp/clickhouse/columns/array.cpp \
		clickhouse-cpp/clickhouse/columns/bool.cpp \
		clickhouse-cpp/clickhouse/columns/column.cpp \
		clickhouse-cpp/clickhouse/columns/date.cpp \
		clickhouse-cpp/clickhouse/columns/decimal.cpp \
		clickhouse-cpp/clickhouse/columns/enum.cpp \
		clickhouse-cpp/clickhouse/columns/factory.cpp \
		clickhouse-cpp/clickhouse/columns/geo.cpp \
		clickhouse-cpp/clickhouse/columns/ip4.cpp \
		clickhouse-cpp/clickhouse/columns/ip6.cpp \
		clickhouse-cpp/clickhouse/columns/itemview.cpp \
		clickhouse-cpp/clickhouse/columns/json.cpp \
		clickhouse-cpp/clickhouse/columns/lowcardinality.cpp \
		clickhouse-cpp/clickhouse/columns/map.cpp \
		clickhouse-cpp/clickhouse/columns/nullable.cpp \
		clickhouse-cpp/clickhouse/columns/numeric.cpp \
		clickhouse-cpp/clickhouse/columns/string.cpp \
		clickhouse-cpp/clickhouse/columns/time.cpp \
		clickhouse-cpp/clickhouse/columns/tuple.cpp \
		clickhouse-cpp/clickhouse/columns/uuid.cpp \
		clickhouse-cpp/clickhouse/types/bignum.cpp \
		clickhouse-cpp/clickhouse/types/type_parser.cpp \
		clickhouse-cpp/clickhouse/types/types.cpp \
		clickhouse-cpp/clickhouse/cityhash/city.cc"

	PHP_ADD_INCLUDE(PHP_EXT_SRCDIR()/src)

	PHP_NEW_EXTENSION(clickhouse, $extension_sources, $ext_shared, , $extension_flags -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1, cxx)

	PHP_ADD_SOURCES_X(PHP_EXT_DIR(clickhouse), $library_sources, -w, shared_objects_clickhouse, yes)
fi