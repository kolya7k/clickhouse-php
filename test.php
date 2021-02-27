<?php

	require_once "secret.inc.php";		// CLICKHOUSE_* defines

	$time = microtime(true);

	$ch = new ClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_PORT);

 	$result = $ch->query("SELECT
 		toUInt32(1),
  		toUInt32(-1),
  		toInt32(-1),
		toUInt64(18446744073709551615),
 		toUInt64(9223372036854775807),
 		toFloat64(1 / 3),
 		toFloat32(1 / 3),
 		toFixedString('test', 8),
 		toString('test'),
 		1 == 1,
 		NULL,
		toInt8OrNull('123'),
		toInt8OrNull('123qwe123')
 	") or trigger_error("Failed to query: ".$ch->error." (".$ch->errno.")", E_USER_ERROR);

	$total = 0;
 	while ($row = $result->fetch_assoc())
 	{
 		$total++;
		print_r($row);
		var_dump($row);
//		exit;
	}

	echo "Total: ".$total."\n";
	echo "Memory: ".memory_get_usage()."\n";

?>