<?php

	require_once "secret.inc.php";		// CLICKHOUSE_* constants

	$failed = 0;

	function check(string $name, $expected, $actual): void
	{
		global $failed;

		if ($expected === $actual)
		{
			echo "ok    {$name}\n";
			return;
		}

		$failed++;
		echo "FAIL  {$name}\n      expected: ".json_encode($expected, JSON_UNESCAPED_UNICODE)."\n      actual:   ".json_encode($actual, JSON_UNESCAPED_UNICODE)."\n";
	}

	function value(ClickHouse $ch, string $expression)
	{
		$result = $ch->query("SELECT {$expression} AS v");
		if (!is_object($result))
			return "QUERY FAILED: ".$ch->error;

		return $result->fetch_assoc()['v'];
	}

	function local(ClickHouse $ch, string $datetime, string $fraction = ""): string
	{
		return date("Y-m-d H:i:s", value($ch, "toUnixTimestamp(toDateTime('{$datetime}'))")).$fraction;
	}

	$time = microtime(true);

	$ch = new ClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_PORT);

	echo "--- integers and floats\n";

	check("UInt8", 1, value($ch, "toUInt8(1)"));
	check("UInt8 wrap", 255, value($ch, "toUInt8(-1)"));
	check("Int8", -1, value($ch, "toInt8(-1)"));
	check("Int64 min", PHP_INT_MIN, value($ch, "toInt64(-9223372036854775808)"));
	check("Int64 max", PHP_INT_MAX, value($ch, "toInt64(9223372036854775807)"));
	check("UInt64 fits", PHP_INT_MAX, value($ch, "toUInt64(9223372036854775807)"));
	check("UInt64 overflow is string", "18446744073709551615", value($ch, "toUInt64(18446744073709551615)"));
	check("Int128 fits", -5, value($ch, "toInt128(-5)"));
	check("Int128 min is string", "-170141183460469231731687303715884105728", value($ch, "toInt128('-170141183460469231731687303715884105728')"));
	check("UInt128 max is string", "340282366920938463463374607431768211455", value($ch, "toUInt128('340282366920938463463374607431768211455')"));
	check("Float64", 0.5, value($ch, "toFloat64(0.5)"));
	check("Float32", 0.25, value($ch, "toFloat32(0.25)"));
	check("Bool", true, value($ch, "true"));
	check("Bool false", false, value($ch, "false"));

	echo "--- strings and decimals\n";

	check("String", "test", value($ch, "toString('test')"));
	check("FixedString padded", "test\0\0\0\0", value($ch, "toFixedString('test', 8)"));
	check("Decimal32", "-0.05", value($ch, "toDecimal32(-0.05, 2)"));
	check("Decimal64 padded", "0.500", value($ch, "toDecimal64(0.5, 3)"));
	check("Decimal128", "-123456789.123", value($ch, "toDecimal128(-123456789.123, 3)"));
	check("Decimal scale 0", "42", value($ch, "toDecimal32(42, 0)"));

	echo "--- dates\n";

	check("Date", "2026-09-17", value($ch, "toDate('2026-09-17')"));
	check("Date32", "1950-05-05", value($ch, "toDate32('1950-05-05')"));
	check("DateTime", local($ch, "2026-09-17 12:34:56"), value($ch, "toDateTime('2026-09-17 12:34:56')"));
	check("DateTime64(3)", local($ch, "2026-09-17 12:34:56", ".789"), value($ch, "toDateTime64('2026-09-17 12:34:56.789', 3)"));
	check("DateTime64(0)", local($ch, "2026-09-17 12:34:56"), value($ch, "toDateTime64('2026-09-17 12:34:56', 0)"));
	check("DateTime64(6) before epoch", date("Y-m-d H:i:s", -1).".500000", value($ch, "toDateTime64('1969-12-31 23:59:59.5', 6, 'UTC')"));

	// Time и Time64 включаются настройкой сессии; на старом сервере она неизвестна
	$has_time = (bool) @$ch->query("SET enable_time_time64_type = 1");
	if ($has_time)
	{
		check("Time", "12:34:56", value($ch, "CAST('12:34:56', 'Time')"));
		check("Time negative", "-01:00:00", value($ch, "CAST('-1:00:00', 'Time')"));
		check("Time over a day", "100:00:00", value($ch, "CAST('100:00:00', 'Time')"));
		check("Time64(3)", "12:34:56.789", value($ch, "CAST('12:34:56.789', 'Time64(3)')"));
		check("Nullable(Time64)", "00:00:01.5", value($ch, "toNullable(CAST('00:00:01.5', 'Time64(1)'))"));
	}
	else
		echo "skip  Time tests: ".$ch->error."\n";

	echo "--- nullable and low cardinality\n";

	check("NULL", null, value($ch, "NULL"));
	check("Nullable Int8", 123, value($ch, "toInt8OrNull('123')"));
	check("Nullable Int8 null", null, value($ch, "toInt8OrNull('123qwe123')"));
	check("LowCardinality(String)", "plain", value($ch, "toLowCardinality('plain')"));
	check("LowCardinality(FixedString)", "fx\0\0", value($ch, "toLowCardinality(toFixedString('fx', 4))"));
	check("LowCardinality(Nullable(String))", "lc", value($ch, "toLowCardinality(toNullable('lc'))"));
	check("LowCardinality(Nullable(String)) null", null, value($ch, "toLowCardinality(CAST(NULL, 'Nullable(String)'))"));

	echo "--- enums, ip, uuid\n";

	check("Enum8", "b", value($ch, "CAST('b', 'Enum8(\\'a\\' = 1, \\'b\\' = 2)')"));
	check("Enum16", "y", value($ch, "CAST('y', 'Enum16(\\'x\\' = 100, \\'y\\' = 200)')"));
	check("IPv4", "87.120.187.114", value($ch, "toIPv4('87.120.187.114')"));
	check("IPv6", "2a0c:b847:ffff:59::a", value($ch, "toIPv6('2a0c:b847:ffff:59::a')"));
	check("UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", value($ch, "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')"));

	echo "--- arrays, tuples, maps, geo\n";

	check("Array(UInt8)", [1, 2, 3], value($ch, "[1, 2, 3]"));
	check("Array empty", [], value($ch, "emptyArrayString()"));
	check("Array(Array)", [[1, 2], [3]], value($ch, "[[1, 2], [3]]"));
	check("Array(Nullable)", [null, 5], value($ch, "[NULL, 5]"));
	check("Array(LowCardinality)", ["q", "w"], value($ch, "[toLowCardinality('q'), toLowCardinality('w')]"));
	check("Tuple", [1, "two", 3.5], value($ch, "(1, 'two', 3.5)"));
	check("Map(String, UInt32)", ["k1" => 10, "k2" => 20], value($ch, "map('k1', 10, 'k2', 20)"));
	check("Map(UInt8, String)", [1 => "one", 2 => "two"], value($ch, "map(1, 'one', 2, 'two')"));
	check("Map(String, Array)", ["a" => [1, 2]], value($ch, "map('a', [1, 2])"));
	check("Point", [1.5, 2.5], value($ch, "(1.5, 2.5)::Point"));
	check("Ring", [[0.0, 0.0], [1.0, 1.0]], value($ch, "[(0., 0.), (1., 1.)]::Ring"));
	check("Polygon", [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0]]], value($ch, "[[(0., 0.), (1., 0.), (1., 1.)]]::Polygon"));

	echo "--- fetch modes and errors\n";

	$result = $ch->query("SELECT number, toString(number) AS text FROM numbers(3)");
	check("num_rows", 3, $result->num_rows);
	check("affected_rows for select", 3, $ch->affected_rows);
	check("fetch_row", [0, "0"], $result->fetch_row());
	check("fetch_array BOTH", [0 => 1, "number" => 1, 1 => "1", "text" => "1"], $result->fetch_array(CLICKHOUSE_BOTH));
	check("fetch_array ASSOC", ["number" => 2, "text" => "2"], $result->fetch_array(CLICKHOUSE_ASSOC));
	check("fetch after end", false, $result->fetch_assoc());

	check("fetch_all NUM", [[0], [1]], $ch->query("SELECT number FROM numbers(2)")->fetch_all(CLICKHOUSE_NUM));
	check("fetch_all ASSOC", [["number" => 0]], $ch->query("SELECT number FROM numbers(1)")->fetch_all(CLICKHOUSE_ASSOC));
	check("fetch_all empty", [], $ch->query("SELECT number FROM numbers(0)")->fetch_all(CLICKHOUSE_ASSOC));
	check("query without result set", true, $ch->query("SET max_threads = 1"));

	check("bad query", false, @$ch->query("SELECT nope FROM nowhere"));
	check("errno set", true, $ch->errno != 0);
	check("error set", true, str_contains($ch->error, "nowhere"));
	check("affected_rows on error", -1, $ch->affected_rows);
	check("errno reset", 0, $ch->query("SELECT 1") ? $ch->errno : -1);
	check("syntax error", false, @$ch->query("SELEC 1"));
	check("syntax error code", 62, $ch->errno);
	check("unknown table", false, @$ch->query("SELECT 1 FROM no_such_table"));
	check("unknown table code", 60, $ch->errno);
	check("unknown database", false, @$ch->query("SELECT 1 FROM no_such_db.t"));
	check("unknown database code", 81, $ch->errno);
	check("connection alive after errors", 1, value($ch, "1"));

	echo "--- connection errors\n";

	$bad = @new ClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USER, "wrong password", CLICKHOUSE_DATABASE, CLICKHOUSE_PORT);
	check("query with wrong password", false, @$bad->query("SELECT 1"));
	check("insert with wrong password", false, @$bad->insert("t", [[1]]));
	$bad = @new ClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, 1);
	check("query with wrong port", false, @$bad->query("SELECT 1"));
	$bad = @new ClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, "no_such_db", CLICKHOUSE_PORT);
	check("query with unknown default database", false, @$bad->query("SELECT 1"));

	echo "--- insert\n";

	$ch->query("SET async_insert = 0");

	$table = "clickhouse_php_test_".getmypid();

	if (!@$ch->query("CREATE TABLE {$table} (
		id UInt64,
		lc LowCardinality(String),
		lcn LowCardinality(Nullable(String)),
		ns Nullable(String),
		i128 Int128,
		u128 UInt128,
		f32 Float32,
		dec Decimal64(3),
		d Date,
		d32 Date32,
		dt DateTime,
		dt64 DateTime64(3),
		uuid UUID,
		ip4 IPv4,
		ip6 IPv6,
		e Enum8('a' = 1, 'b' = 2),
		arr Array(UInt8),
		arr_ns Array(Nullable(String)),
		arr_lc Array(LowCardinality(String)),
		tup Tuple(UInt8, String),
		m Map(String, UInt32),
		pt Point,
		ring Ring,
		fx FixedString(4),
		b Bool".($has_time ? ",
		t Time,
		t64 Time64(3)" : "")."
	) ENGINE = Memory"))
		echo "skip  insert tests: ".$ch->error."\n";
	else
	{
		$fields = ["id", "lc", "lcn", "ns", "i128", "u128", "f32", "dec", "d", "d32", "dt", "dt64", "uuid", "ip4", "ip6", "e", "arr", "arr_ns", "arr_lc", "tup", "m", "pt", "ring", "fx", "b"];
		$time_values = [["12:34:56", "-00:00:01.5"], [3600, 1.25]];

		if ($has_time)
			array_push($fields, "t", "t64");

		$inserted = $ch->insert($table, [
			[1, "one", "x", "s", "-170141183460469231731687303715884105728", "340282366920938463463374607431768211455", 1.5, "12.345", "2026-09-17", "1950-05-05", "2026-09-17 12:34:56", "2026-09-17 12:34:56.789", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", "87.120.187.114", "2a0c:b847:ffff:59::a", "b", [1, 2, 3], ["a", null], ["q", "w"], [7, "seven"], ["k1" => 10, "k2" => 20], [1.5, 2.5], [[0, 0], [1, 1]], "ab", true, ...($has_time ? $time_values[0] : [])],
			[2, "two", null, null, 5, 6, 2, 0.5, 20000, 20000, 1758000000, 1758000000.25, "00000000-0000-0000-0000-000000000000", 16909060, "::1", 1, [], [null], [], [0, ""], [], [0, 0], [], "", false, ...($has_time ? $time_values[1] : [])],
		], $fields);
		check("insert numeric keys", true, $inserted);
		check("affected_rows for insert", 2, $ch->affected_rows);

		check("insert assoc keys", true, $ch->insert($table, [
			["id" => 3, "lc" => "three", "lcn" => "z", "ns" => "", "i128" => -1, "u128" => 1, "f32" => 0.0, "dec" => "-0.001", "d" => "1970-01-01", "d32" => "1970-01-01", "dt" => "1970-01-01 03:00:00", "dt64" => "2000-01-01 00:00:00 UTC", "uuid" => "ffffffff-ffff-ffff-ffff-ffffffffffff", "ip4" => "0.0.0.0", "ip6" => "::", "e" => "a", "arr" => [255], "arr_ns" => [], "arr_lc" => ["x"], "tup" => [1, "a"], "m" => ["only" => 1], "pt" => [0, 0], "ring" => [[1, 2]], "fx" => "abcd", "b" => false] + ($has_time ? ["t" => "00:00:00", "t64" => "00:00:00"] : []),
		]));

		$result = $ch->query("SELECT * FROM {$table} ORDER BY id");
		$rows = is_object($result) ? $result->fetch_all(CLICKHOUSE_ASSOC) : [];
		check("rows inserted", 3, count($rows));

		if (count($rows) == 3)
		{
			$row = $rows[0];
			check("read lc", "one", $row['lc']);
			check("read lcn", "x", $row['lcn']);
			check("read i128 string", "-170141183460469231731687303715884105728", $row['i128']);
			check("read u128 string", "340282366920938463463374607431768211455", $row['u128']);
			check("read f32", 1.5, $row['f32']);
			check("read dec", "12.345", $row['dec']);
			check("read d", "2026-09-17", $row['d']);
			check("read d32", "1950-05-05", $row['d32']);
			check("read dt", "2026-09-17 12:34:56", $row['dt']);
			check("read dt64", "2026-09-17 12:34:56.789", $row['dt64']);
			check("read uuid", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", $row['uuid']);
			check("read ip4", "87.120.187.114", $row['ip4']);
			check("read ip6", "2a0c:b847:ffff:59::a", $row['ip6']);
			check("read enum", "b", $row['e']);
			check("read arr", [1, 2, 3], $row['arr']);
			check("read arr_ns", ["a", null], $row['arr_ns']);
			check("read arr_lc", ["q", "w"], $row['arr_lc']);
			check("read tup", [7, "seven"], $row['tup']);
			check("read map", ["k1" => 10, "k2" => 20], $row['m']);
			check("read point", [1.5, 2.5], $row['pt']);
			check("read ring", [[0.0, 0.0], [1.0, 1.0]], $row['ring']);
			check("read fx", "ab\0\0", $row['fx']);
			check("read bool", true, $row['b']);

			if ($has_time)
			{
				check("read time", "12:34:56", $row['t']);
				check("read time64 negative", "-00:00:01.500", $row['t64']);
				check("read time from seconds", "01:00:00", $rows[1]['t']);
				check("read time64 from float", "00:00:01.250", $rows[1]['t64']);
			}

			$row = $rows[1];
			check("read lcn null", null, $row['lcn']);
			check("read ns null", null, $row['ns']);
			check("read i128 small", 5, $row['i128']);
			check("read dec float", "0.500", $row['dec']);
			check("read d from days", "2024-10-04", $row['d']);
			check("read dt from timestamp", date("Y-m-d H:i:s", 1758000000), $row['dt']);
			check("read dt64 from float", date("Y-m-d H:i:s", 1758000000).".250", $row['dt64']);
			check("read ip4 from int", "1.2.3.4", $row['ip4']);
			check("read enum from value", "a", $row['e']);
			check("read empty arrays", [[], [null], []], [$row['arr'], $row['arr_ns'], $row['arr_lc']]);
			check("read empty map", [], $row['m']);
			check("read empty ring", [], $row['ring']);
			check("read empty fixed", "\0\0\0\0", $row['fx']);
			check("read bool false", false, $row['b']);

			check("read dec negative small", "-0.001", $rows[2]['dec']);
			check("read map single", ["only" => 1], $rows[2]['m']);
			check("read dt64 from string with zone", date("Y-m-d H:i:s", 946684800).".000", $rows[2]['dt64']);
		}

		echo "--- insert errors\n";

		check("type mismatch", false, @$ch->insert($table, [["id" => "not a number"]]));
		check("unknown enum name", false, @$ch->insert($table, [["id" => 4, "e" => "zzz"]]));
		check("error after bad enum", true, $ch->error != "");
		check("bad uuid", false, @$ch->insert($table, [["id" => 5, "uuid" => "nope"]]));
		check("bad datetime", false, @$ch->insert($table, [["id" => 11, "dt" => "not a date"]]));
		check("bad ip", false, @$ch->insert($table, [["id" => 6, "ip4" => "300.1.1.1"]]));
		check("mixed keys", false, @$ch->insert($table, [["id" => 7, "lc"], ["id" => 8]]));
		check("unknown column", false, @$ch->insert($table, [["nope" => 1]]));
		check("wrong tuple size", false, @$ch->insert($table, [["id" => 9, "tup" => [1]]]));
		check("fixed string too long", false, @$ch->insert($table, [["id" => 10, "fx" => "abcde"]]));
		check("empty values", false, @$ch->insert($table, []));
		check("null into non-nullable", false, @$ch->insert($table, [["id" => null]]));
		check("insert into unknown table", false, @$ch->insert("no_such_table", [["id" => 1]]));
		check("insert unknown table code", 60, $ch->errno);
		check("insert into unknown database", false, @$ch->insert("no_such_db.t", [["id" => 1]]));
		check("insert unknown database code", 81, $ch->errno);
		check("table still has 3 rows", 3, $ch->query("SELECT count() AS c FROM {$table}")->fetch_assoc()['c']);

		$ch->query("DROP TABLE {$table}");
	}

	echo "\n".($failed == 0 ? "ALL PASSED" : "{$failed} FAILED").", ".round(microtime(true) - $time, 2)." s, memory ".memory_get_usage()."\n";

	exit($failed == 0 ? 0 : 1);

?>