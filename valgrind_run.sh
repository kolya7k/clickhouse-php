USE_ZEND_ALLOC=0 ZEND_DONT_UNLOAD_MODULES=1 valgrind --leak-check=full --track-origins=yes php -dextension=modules/clickhouse.so test.php

#perf record --call-graph dwarf,65528 php -dextension=modules/clickhouse.so test.php

#perf report -g fractal