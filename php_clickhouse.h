#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#pragma GCC diagnostic ignored "-Wfloat-equal"
#include <php.h>
#include <ext/standard/info.h>
#pragma GCC diagnostic pop

extern zend_module_entry clickhouse_module_entry;
#define phpext_clickhouse_ptr &clickhouse_module_entry

ZEND_BEGIN_MODULE_GLOBALS(clickhouse)
//
ZEND_END_MODULE_GLOBALS(clickhouse)

#ifdef ZTS
#define CLICKHOUSE_G(v) TSRMG(clickhouse_globals_id, zend_clickhouse_globals*, v)
#else
#define CLICKHOUSE_G(v) (clickhouse_globals.v)
#endif

#if defined(ZTS) && defined(COMPILE_DL_CLICKHOUSE)
ZEND_TSRMLS_CACHE_EXTERN()
#endif