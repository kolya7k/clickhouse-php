# ClickHouse PHP extension

PHP extension for [Yandex ClickHouse](https://clickhouse.yandex/)

Supports PHP 7.0+

Written in C++ using [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) library

## Dependencies
* PHP 7.0+
* GCC 10+
* liblz4
* libzstd

## Building

clickhouse-cpp is compiled into the extension from the submodule (with its cityhash), lz4 and zstd are taken from the system.

Remote build from Windows, same as the game servers: `bash remote_build.sh` uploads the tree to the host from `.env.local.defaults` (`REMOTE_USER` in `.env.local`) and runs `cmake` there; `CMakeLists.txt` copies the sources into `build/<type>/ext` and runs `phpize`, `configure` and `make` in that copy, then prints the module version. The `install-clickhouse-php` target runs `make install` (needs root). CLion builds the same way through the remote toolchain and the `Release_debug` profile from `.idea/cmake.xml`.
```sh
$ git clone --recursive --depth=1 https://github.com/kolya7k/clickhouse-php.git
$ cd clickhouse-php
$ phpize
$ ./configure
$ make -j 16
$ make install
```

## Supported types
* Int8, Int16, Int32, Int64, Int128
* UInt8, UInt16, UInt32, UInt64, UInt128, Bool (read as int)
* Float32, Float64
* String, FixedString<N>
* Date, Date32, DateTime, DateTime64 (strings in the local time of the PHP host, like mysqli)
* Decimal, Decimal32, Decimal64, Decimal128 (strings)
* UUID, IPv4, IPv6 (strings)
* Enum8, Enum16 (names)
* Nullable<T>
* LowCardinality<T> for String, FixedString and Nullable of them (clickhouse-cpp does not support numeric LowCardinality)
* Array<T>, Tuple, Map (PHP arrays, nested to any depth)
* Point, Ring, Polygon, MultiPolygon (nested arrays of [x, y])

Values outside of PHP int range (UInt64, Int128, UInt128) are returned as strings and accepted as strings on insert.

## Limitations and difference from mysqli
* No MYSQLI_USE_RESULT logic, all data loaded into memory before using it in PHP code
* More complex insert logic than in mysqli due to clickhouse-cpp limitations (see example below)
* Not all ClickHouse features have been implemented yet, in development
* Not all types are supported yet, also in development

## TODO
* Parse INSERT query like the ClickHouse command line utility does
* Support for other ClickHouse formats
* Tests
* Benchmarks

## Example

```php
<?php

	$ch = new ClickHouse("127.0.0.1", "default", "", "default", 9000);

	$result = $ch->query("SELECT
		toUInt8(1),
		toUInt8(-1),
		toUInt16(1),
		toUInt16(-1),
		toUInt32(1),
		toUInt32(-1),
		toUInt64(1),
		toUInt64(-1),
		toUInt64(9223372036854775807),
		toUInt64(18446744073709551615),
		toInt8(1),
		toInt8(-1),
		toInt16(1),
		toInt16(-1),
		toInt32(1),
		toInt32(-1),
		toInt64(1),
		toInt64(-1),
		toInt64(9223372036854775807),
		toInt64(-9223372036854775808),
		toInt64(18446744073709551615),
		toFloat64(1 / 3),
		toFloat32(1 / 3),
		toFixedString('test', 8),
		toString('test'),
		toDate('2021-01-01'),
		toDateTime('2021-01-01 00:00:00'),
		toInt8OrNull('123'),
		toInt8OrNull('123qwe123'),
		1 == 1,
		NULL,
		toDecimal128(123456789.123, 3)
	") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_ERROR);

	$row = $result->fetch_assoc();
	var_dump($row);

	$ch->query("CREATE TABLE IF NOT EXISTS test (
		id UInt64,
		name String,
		key FixedString(5),
		nullable_date Nullable(Date),
		nullable_fixed Nullable(FixedString(15))
	) ENGINE = Memory") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	// Index-based data, columns names in separated array
	$ch->insert("test",
		array(
			array(1, "a", "aa", "2020-01-01", "test1"),
			array(2, "b", "bb", NULL, "test2"),
			array(3, "c", "cc", "2020-01-03", NULL)
		),
		array("id", "name", "key", "nullable_date", "nullable_fixed")
	) or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	// Associative array data, columns names inside data array, slower than index-based
	$ch->insert("test",
		array(
			array('id' => 4, 'name' => "d", 'key' => "dd", 'nullable_date' => "2020-01-04",	'nullable_fixed' => NULL),
			array('id' => 5, 'name' => "e", 'key' => "ee", 'nullable_date' => NULL,		'nullable_fixed' => "test5"),
			array('id' => 6, 'name' => "f", 'key' => "ff", 'nullable_date' => "2020-01-06",	'nullable_fixed' => "test6")
		)
	) or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	$result = $ch->query("SELECT * FROM test") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);
	while ($row = $result->fetch_assoc())
		var_dump($row);

	$ch->query("DROP TABLE test") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	echo "Memory: ".memory_get_usage()."\n";

?>
```
