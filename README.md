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
		toFloat64(1 / 3),
		toFloat32(1 / 3),
		toFixedString('test', 8),
		toString('test'),
		toDate('2021-01-01'),
		toDateTime('2021-01-01 00:00:00'),
		toInt8OrNull('123'),
		toInt8OrNull('123qwe123'),
		1 == 1,
		NULL
	") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_ERROR);

	while ($row = $result->fetch_assoc())
		print_r($row);

	$ch->query("CREATE TABLE IF NOT EXISTS numbers (id UInt64, name String, value FixedString(3)) ENGINE = Memory") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	$ch->insert("numbers",
		array(
			[1, "a", "aa\0"],						// FixedString needs full size
			[2, "b", "bb\0"],
			[3, "c", "cc\0"]
		),
		array("UInt32", "String", "FixedString"),				// Types information needed by ClicKHouse API
		array("id", "name", "key")						// Columns names in separated array
	) or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	$ch->insert("numbers",
		array(
			['id' => 4, 'name' => "d", 'key' => "aa\0"],			// Columns names inside data array
			['id' => 5, 'name' => "e", 'key' => "bb\0"],			// FixedString needs full size
			['id' => 6, 'name' => "f", 'key' => "cc\0"]
		),
		array('id' => "UInt32", 'name' => "String", 'key' => "FixedString")	// Types information needed by ClicKHouse API
	) or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	$result = $ch->query("SELECT * FROM numbers") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);
	while ($row = $result->fetch_assoc())
		print_r($row);

	$ch->query("DROP TABLE numbers") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	echo "Memory: ".memory_get_usage()."\n";

?>
