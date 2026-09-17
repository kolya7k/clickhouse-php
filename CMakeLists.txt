cmake_minimum_required(VERSION 3.29)

project("Clickhouse PHP" LANGUAGES CXX)

set(EXT_DIR ${CMAKE_BINARY_DIR}/ext)
set(EXT_MODULE ${EXT_DIR}/modules/clickhouse.so)

file(MAKE_DIRECTORY ${EXT_DIR})

add_custom_target(clickhouse-php ALL
	COMMAND rsync -a --exclude build --exclude .git --exclude .idea ${CMAKE_SOURCE_DIR}/ ${EXT_DIR}/
	COMMAND sh -c "test Makefile -nt config.m4 || (phpize && ./configure --enable-clickhouse)"
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

target_include_directories(clickhouse-php-model PRIVATE src clickhouse-cpp clickhouse-cpp/clickhouse/cityhash ${PHP_INCLUDE_DIR} ${PHP_INCLUDE_DIR}/main ${PHP_INCLUDE_DIR}/Zend ${PHP_INCLUDE_DIR}/TSRM)
target_compile_options(clickhouse-php-model PRIVATE -include defines.h -std=gnu++2a -Wall -Wextra -Wdeprecated -Wno-deprecated-declarations -Wno-unused-parameter -Wredundant-decls -Wlogical-op -Wtrampolines -Wduplicated-cond -Wsuggest-override -Wdouble-promotion -Wno-unknown-pragmas -Wcast-qual)