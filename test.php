<?php

	require_once "secret.inc.php";		// CLICKHOUSE_* defines

	$time = microtime(true);

	$ch = new ClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_PORT);

	$query = "SELECT
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
	";

	$result = $ch->query($query) or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	$row = $result->fetch_assoc();
	var_dump($row);

/*
	$result = $ch->query("SELECT * FROM bottle.stats_actions LIMIT 1000000") or trigger_error("Failed to run query: ".$ch->error." (".$ch->errno.")", E_USER_WARNING);

	var_dump($result->num_rows);

	$total = 0;
	while ($row = $result->fetch_assoc())
		$total++;
*/

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