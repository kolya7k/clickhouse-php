# ClickHouse PHP extension

PHP extension for [Yandex ClickHouse](https://clickhouse.yandex/)

Written in C++ using clickhouse-cpp library

## Building

Install [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) first

```sh
$ phpize && ./configure
$ make
$ make install
```

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
	{
		print_r($row);
		var_dump($row);
	}

	echo "Total: ".$total."\n";
	echo "Memory: ".memory_get_usage()."\n";

?>
