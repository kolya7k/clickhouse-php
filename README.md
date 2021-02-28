# ClickHouse PHP extension

PHP extension for [Yandex ClickHouse](https://clickhouse.yandex/)

Supports PHP 7.0+

Written in C++ using [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) library

## Building

Install [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) first
Then do

```sh
$ phpize && ./configure
$ make
$ make install
```

## Supported types
* Int8
* Int16
* Int32
* Int64
* UInt8
* UInt16
* UInt32
* UInt64
* Float32
* Float64
* String
* FixedString
* DateTime
* Date

## Limitations and differnce from mysqli
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
		toUInt64(18446744073709551615),
		toUInt64(9223372036854775807),
		toFloat32(1 / 3),
		toFloat64(1 / 3),
		toFixedString('test', 8),
		toString('test'),
		toDate('2021-01-01'),
		toDateTime('2021-01-01 00:00:00'),
		toInt8OrNull('123'),
		toInt8OrNull('123qwe123'),
		1 == 1,
		NULL
	") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_ERROR);

	$row = $result->fetch_assoc();
	var_dump($row);

	$ch->query("CREATE TABLE IF NOT EXISTS numbers (
		id UInt64,
		name String,
		key FixedString(5),
		nullable Nullable(Date)
	) ENGINE = Memory") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	// Index-based data
	$ch->insert("numbers",
		array(
			array(1, "a", "aa", "2020-01-01"),
			array(2, "b", "bb", NULL),
			array(3, "c", "cc", "2020-01-03")
		),

		// Types information needed by ClicKHouse API
		array("UInt64", "String", "FixedString(5)", "Nullable(Date)"),

		// Columns names in separated array
		array("id", "name", "key", "nullable")
	) or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	// Associative array data, slower than index-based
	$ch->insert("numbers",
		array(
			// Columns names inside data array
			array('id' => 4, 'name' => "d", 'key' => "dd", 'nullable' => "2020-01-04"),
			array('id' => 5, 'name' => "e", 'key' => "ee", 'nullable' => NULL),
			array('id' => 6, 'name' => "f", 'key' => "ff", 'nullable' => "2020-01-06")
		),

		// Types information needed by ClicKHouse API
		array('id' => "UInt64", 'name' => "String", 'key' => "FixedString(5)", 'nullable' => "Nullable(Date)")
	) or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	$result = $ch->query("SELECT * FROM numbers") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);
	while ($row = $result->fetch_assoc())
		print_r($row);

	$ch->query("DROP TABLE numbers") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	echo "Memory: ".memory_get_usage()."\n";

?>
```