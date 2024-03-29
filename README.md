# ClickHouse PHP extension

PHP extension for [Yandex ClickHouse](https://clickhouse.yandex/)

Supports PHP 7.0+

Written in C++ using [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) library

## Dependencies
* PHP 7.0+
* GCC 10+
* openssl
* liblz4
* libabsl

## Building
```sh
$ git clone --recursive --depth=1 https://github.com/kolya7k/clickhouse-php.git
$ cd clickhouse-php
$ phpize
$ ./configure
$ make -j 16
$ make install
```

## Supported types
* Int8, Int16, Int32, Int64
* UInt8, UInt16, UInt32, UInt64
* Float32, Float64
* String
* FixedString\<N\>
* DateTime
* Date
* Decimal (only for reading)
* Nullable\<T\> for all previous types

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
