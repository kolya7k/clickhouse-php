# ClickHouse PHP extension

PHP extension for [ClickHouse](https://clickhouse.com/) over the native TCP protocol.
Written in C++ on top of [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp), the API mirrors mysqli.

## Requirements
* PHP 8.0+ (tested with 8.4)
* GCC 10+ (C++20)
* liblz4, libzstd
* clickhouse-cpp as a git submodule, compiled into the extension together with its cityhash

## Building

```sh
$ git clone --recursive https://github.com/kolya7k/clickhouse-php.git
$ cd clickhouse-php
$ phpize
$ ./configure --enable-clickhouse
$ make -j 16
$ make install
```

Then add `extension=clickhouse.so` to `php.ini`. The module is compiled with `-march=native`, so build it on the host it will run on.

`CMakeLists.txt` is a wrapper for IDEs that speak CMake: the `clickhouse-php` target runs the same `phpize`, `configure` and `make` in `build/<type>/ext` and prints the module version, `install-clickhouse-php` runs `make install` there.

## API

```php
$ch = new ClickHouse(string $host = "127.0.0.1", string $username = "default", string $passwd = "", string $dbname = "default", int $port = 9000);

$ch->query(string $query): ClickHouseResult|bool;                // result set → ClickHouseResult, no result set → true, error → false
$ch->insert(string $table, array $rows, array $fields = []): bool;
$ch->errno;                                                       // ClickHouse error code, 0 for client-side errors
$ch->error;                                                       // error message
$ch->affected_rows;                                               // rows in the result set or rows inserted, -1 on error

$result->num_rows;
$result->fetch_assoc(): array|false;
$result->fetch_row(): array|false;
$result->fetch_array(int $type = CLICKHOUSE_BOTH): array|false;
$result->fetch_all(int $type = CLICKHOUSE_NUM): array;
```

Every error raises a PHP warning and makes the call return `false`, so `@` and the `errno`/`error` properties work like in mysqli.
A failed connection raises a warning in the constructor, all later calls on that object return `false`.
The connection uses LZ4 compression, TLS is not supported.

## Supported types
* Int8, Int16, Int32, Int64, Int128
* UInt8, UInt16, UInt32, UInt64, UInt128
* Bool (PHP bool)
* Float32, Float64
* String, FixedString<N>
* Date, Date32, DateTime, DateTime64 (strings in the local time of the PHP host, like mysqli; on insert any `strtotime` format, a Unix timestamp, or float seconds for DateTime64)
* Time, Time64 (strings like `12:34:56.789`, hours are not wrapped, negative values keep the sign; the server needs `enable_time_time64_type = 1`)
* Decimal, Decimal32, Decimal64, Decimal128 (strings)
* UUID, IPv4, IPv6 (strings; IPv4 also accepts an `ip2long` integer on insert)
* Enum8, Enum16 (names; on insert a name or a value)
* Nullable<T>
* LowCardinality<T> for String, FixedString and Nullable of them (clickhouse-cpp does not support numeric LowCardinality)
* Array<T>, Tuple, Map (PHP arrays, nested to any depth; Map is an associative array)
* Point, Ring, Polygon, MultiPolygon (nested arrays of [x, y])

Values outside of PHP int range (UInt64, Int128, UInt128) are returned as strings and accepted as strings on insert.
Any other type (JSON, Variant, Dynamic, aggregate function states) raises a warning.

## Insert

`insert()` sends a native block, not SQL: `$rows` is a list of rows, either indexed (column names in `$fields`, faster) or associative (column names as keys). All rows must have the same set of columns, values are converted by the column type from the table description (see the example below). `INSERT ... VALUES` through `query()` sends no data and inserts nothing, use `insert()`.

If the user profile has `async_insert = 1` without `wait_for_async_insert`, a `SELECT` right after `insert()` may not see the rows yet: that is ClickHouse behaviour, not a bug of the extension.

## Limitations and difference from mysqli
* No MYSQLI_USE_RESULT logic, the whole result set is loaded into memory before it is used from PHP
* Insert goes through `insert()` with the table description, not through SQL
* No prepared statements, no TLS, no connection options beyond host, port, user, password and database

## TODO
* Parse `INSERT ... VALUES` in `query()` like the ClickHouse command line client does
* Benchmarks

## Tests

`test.php` checks every supported type on read and insert, fetch modes, error handling and connection errors. It needs `secret.inc.php`
with `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_PORT` constants and a user that may create tables in that database (a temporary `Memory` table is created and dropped):

```sh
$ php -n -d date.timezone=UTC -d extension=modules/clickhouse.so test.php
```

## Example

```php
<?php

	$ch = new ClickHouse("127.0.0.1", "default", "", "default", 9000);

	$result = $ch->query("SELECT
		toUInt64(18446744073709551615) AS big,
		toFloat32(1 / 3) AS f,
		toFixedString('test', 8) AS fx,
		toDateTime('2021-01-01 00:00:00') AS dt,
		toInt8OrNull('123qwe123') AS n,
		true AS b,
		[1, 2, 3] AS arr,
		map('a', 1) AS m,
		toDecimal128(123456789.123, 3) AS dec
	") or trigger_error("Failed to run query: {$ch->error} ({$ch->errno})", E_USER_ERROR);

	var_dump($result->fetch_assoc());

	$ch->query("CREATE TABLE IF NOT EXISTS test (
		id UInt64,
		name String,
		key FixedString(5),
		nullable_date Nullable(Date),
		nullable_fixed Nullable(FixedString(15))
	) ENGINE = Memory") or trigger_error("Failed to run query: {$ch->error} ({$ch->errno})", E_USER_WARNING);

	// Indexed rows, column names in a separate array
	$ch->insert("test", [
		[1, "a", "aa", "2020-01-01", "test1"],
		[2, "b", "bb", null, "test2"],
		[3, "c", "cc", "2020-01-03", null],
	], ["id", "name", "key", "nullable_date", "nullable_fixed"]) or trigger_error("Failed to insert: {$ch->error} ({$ch->errno})", E_USER_WARNING);

	// Associative rows, column names inside the rows, slower than indexed
	$ch->insert("test", [
		["id" => 4, "name" => "d", "key" => "dd", "nullable_date" => "2020-01-04", "nullable_fixed" => null],
		["id" => 5, "name" => "e", "key" => "ee", "nullable_date" => null, "nullable_fixed" => "test5"],
	]) or trigger_error("Failed to insert: {$ch->error} ({$ch->errno})", E_USER_WARNING);

	$result = $ch->query("SELECT * FROM test ORDER BY id") or trigger_error("Failed to run query: {$ch->error} ({$ch->errno})", E_USER_WARNING);
	while ($row = $result->fetch_assoc())
		var_dump($row);

	$ch->query("DROP TABLE test");
```
