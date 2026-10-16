#pragma once

#include <php.h>
extern "C"
{
#include <ext/date/php_date.h>
}

extern zend_module_entry clickhouse_module_entry;
#define PHPEXT_CLICKHOUSE_PTR &clickhouse_module_entry

inline zend_class_entry *clickhouse_class_entry = nullptr;
inline zend_class_entry *clickhouse_result_class_entry = nullptr;

inline zend_object_handlers clickhouse_object_handlers;
inline zend_object_handlers clickhouse_object_result_handlers;

#if defined(ZTS) && defined(COMPILE_DL_CLICKHOUSE)
ZEND_TSRMLS_CACHE_EXTERN()
#endif