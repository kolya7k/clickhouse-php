cmake_minimum_required(VERSION 3.29)

if(CLANG STREQUAL "ON")
	find_program(CLANG_CXX_COMPILER clang++)
	if(CLANG_CXX_COMPILER)
		set(CMAKE_CXX_COMPILER "${CLANG_CXX_COMPILER}" CACHE FILEPATH "C++ compiler" FORCE)
	else()
		message(FATAL_ERROR "CLANG=ON but clang++ not found")
	endif()
endif()

project("Clickhouse PHP" LANGUAGES CXX)

set(EXT_DIR ${CMAKE_BINARY_DIR}/ext)
set(EXT_MODULE ${EXT_DIR}/modules/clickhouse.so)

file(MAKE_DIRECTORY ${EXT_DIR})

add_custom_target(clickhouse-php ALL
	COMMAND rsync -a --exclude build --exclude .git --exclude .idea ${CMAKE_SOURCE_DIR}/ ${EXT_DIR}/
	COMMAND sh -c "test Makefile -nt config.m4 || (phpize && ./configure --enable-clickhouse CXX=${CMAKE_CXX_COMPILER})"
	COMMAND make -j 16
	WORKING_DIRECTORY ${EXT_DIR}
	USES_TERMINAL
	VERBATIM
	COMMENT "phpize, configure, make in ${EXT_DIR}"
)

add_custom_target(install-clickhouse-php
	COMMAND make install
	WORKING_DIRECTORY ${EXT_DIR}
	DEPENDS clickhouse-php
	USES_TERMINAL
	VERBATIM
)

file(WRITE ${CMAKE_BINARY_DIR}/clickhouse-php "#!/bin/sh\nexec php -n -d extension=${EXT_MODULE} -r 'echo \"clickhouse \", phpversion(\"clickhouse\"), PHP_EOL;'\n")
file(CHMOD ${CMAKE_BINARY_DIR}/clickhouse-php PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

execute_process(COMMAND php-config --include-dir OUTPUT_VARIABLE PHP_INCLUDE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)

add_library(clickhouse-php-model EXCLUDE_FROM_ALL src/clickhouse.cpp src/util.cpp src/ClickHouseDB.cpp src/ClickHouseResult.cpp)

target_include_directories(clickhouse-php-model PRIVATE src)
target_include_directories(clickhouse-php-model SYSTEM PRIVATE clickhouse-cpp clickhouse-cpp/clickhouse/cityhash ${PHP_INCLUDE_DIR} ${PHP_INCLUDE_DIR}/main ${PHP_INCLUDE_DIR}/Zend ${PHP_INCLUDE_DIR}/TSRM)
target_compile_options(clickhouse-php-model PRIVATE -include defines.h -std=gnu++2a -Wall -Wextra -Werror -Wdeprecated -Wconversion -Wredundant-decls -Wfloat-equal -Wcast-qual -Wdouble-promotion -Wmissing-include-dirs -Wundef -Wuninitialized -Wzero-as-null-pointer-constant -Wnon-virtual-dtor -Woverloaded-virtual -pedantic -pedantic-errors -Wno-unused-parameter)

if(CLANG STREQUAL "ON")
	target_compile_options(clickhouse-php-model PRIVATE -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-sign-conversion -Wno-padded -Wno-global-constructors -Wno-reserved-identifier -Wno-exit-time-destructors -Wno-newline-eof -Wno-switch-enum -Wno-old-style-cast -Wno-cast-align -Wno-disabled-macro-expansion -Wno-unused-macros -Wno-class-varargs -Wno-format-nonliteral -Wno-shadow-field-in-constructor -Wno-ctad-maybe-unsupported -Wno-switch-default -Wno-missing-braces -Wno-unsafe-buffer-usage -Wno-nrvo -Wno-date-time -Wno-missing-prototypes -Wno-lifetime-safety-suggestions -Wno-lifetime-safety-invalidation)
else()
	target_compile_options(clickhouse-php-model PRIVATE -Wconditionally-supported -Wcast-align=strict -Wlogical-op -Wuseless-cast -Wtrampolines -Wduplicated-cond -Wsuggest-override -Wno-unknown-pragmas -Wformat=2 -Wnull-dereference -Wduplicated-branches -Wimplicit-fallthrough=5 -Warray-bounds=2 -Wstrict-overflow=2 -Wuse-after-free=3 -Wbidi-chars=any -Wdangling-reference)
endif()