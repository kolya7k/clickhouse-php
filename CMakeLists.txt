cmake_minimum_required(VERSION 3.23)
project("clickhouse-php" CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SOURCE_FILES clickhouse.cpp src/util.cpp src/ClickHouse.cpp src/ClickHouseResult.cpp)

add_compile_options(-fPIC -mno-sse4.2 -mno-sse4.1 -O2 -g3 -std=gnu++2a -Wall -Wextra -Wdeprecated -Wno-unused-parameter -Wredundant-decls -Wlogical-op -Wtrampolines -Wduplicated-cond -Wsuggest-override -Wdouble-promotion -Wno-unknown-pragmas -Wcast-qual -pedantic -pedantic-errors -fno-omit-frame-pointer -include defines.h)
add_link_options(-fPIC -mno-sse4.2 -mno-sse4.1 -O2 -g3 -Wl,--export-dynamic -fno-omit-frame-pointer)

execute_process (
        COMMAND php-config --include-dir
        OUTPUT_VARIABLE PHP_SOURCE
)
string(REGEX REPLACE "\n$" "" PHP_SOURCE "${PHP_SOURCE}")

message("Using source directory: ${PHP_SOURCE}")

include_directories(${PHP_SOURCE})
include_directories(${PHP_SOURCE}/main)
include_directories(${PHP_SOURCE}/Zend)
include_directories(${PHP_SOURCE}/TSRM)

include_directories(.)
include_directories(src)
include_directories(clickhouse-cpp)

add_custom_target(configure
	COMMAND phpize && ./configure
	DEPENDS ${SOURCE_FILES}
	WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

add_custom_target(make
	COMMAND make
	DEPENDS ${SOURCE_FILES}
	WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

add_custom_target(cleanall
	COMMAND make clean
	DEPENDS ${SOURCE_FILES}
	WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

add_library(___ EXCLUDE_FROM_ALL ${SOURCE_FILES})