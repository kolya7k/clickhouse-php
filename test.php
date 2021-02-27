<?php

	require_once "secret.inc.php";		// CLICKHOUSE_* defines

	$time = microtime(true);

	$ch = new ClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_PORT);

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
		toInt8(1),
		toInt8(-1),
		toInt16(1),
		toInt16(-1),
		toInt32(1),
		toInt32(-1),
		toInt64(1),
		toInt64(-1),
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

	echo "Memory: ".memory_get_usage()."\n";

?>