#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "ClickHouseDB.h"
#include "ClickHouseResult.h"

#include <ext/standard/info.h>

static constexpr auto MODULE_VERSION = "1.1.0";

namespace
{

	struct ClickHouseObject
	{
		ClickHouseDB *impl;
		zend_object std;
	};

}

template<class T>
static auto object_from(zend_object *obj) -> T*
{
	return static_cast<T*>(static_cast<void*>(reinterpret_cast<char*>(obj) - XtOffsetOf(T, std)));
}

#define Z_CLICKHOUSE(zv) object_from<ClickHouseObject>(zv)
#define Z_CLICKHOUSE_P(zv) Z_CLICKHOUSE(Z_OBJ_P(zv))

#define Z_CLICKHOUSE_RESULT(zv) object_from<ClickHouseResultObject>(zv)
#define Z_CLICKHOUSE_RESULT_P(zv) Z_CLICKHOUSE_RESULT(Z_OBJ_P(zv))

__inline static auto clickhouse_new(zend_class_entry *ce) -> zend_object*
{
	auto obj = static_cast<ClickHouseObject*>(zend_object_alloc(sizeof(ClickHouseObject), ce));

	zend_object_std_init(&obj->std, ce);
	object_properties_init(&obj->std, ce);

	obj->std.handlers = &clickhouse_object_handlers;

	obj->impl = new ClickHouseDB(&obj->std);

	return &obj->std;
}

__inline static void clickhouse_result_free(zend_object *obj)
{
	auto ch_obj = Z_CLICKHOUSE_RESULT(obj);

	delete ch_obj->impl;
	ch_obj->impl = nullptr;
}

__inline static void clickhouse_free(zend_object *obj)
{
	auto ch_obj = Z_CLICKHOUSE(obj);

	delete ch_obj->impl;
	ch_obj->impl = nullptr;
}

ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_CONSTRUCT, 0, 0, 0)
	ZEND_ARG_TYPE_INFO(0, host, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, username, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, passwd, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, dbname, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, port, IS_LONG, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseObject, __construct)
{
	zend_string *host = nullptr;
	zend_string *username = nullptr;
	zend_string *passwd = nullptr;
	zend_string *dbname = nullptr;
	zend_long port = 0;

	ZEND_PARSE_PARAMETERS_START(0, 5)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR(host)
		Z_PARAM_STR(username)
		Z_PARAM_STR(passwd)
		Z_PARAM_STR(dbname)
		Z_PARAM_LONG(port)
	ZEND_PARSE_PARAMETERS_END();

	auto ch = Z_CLICKHOUSE_P(ZEND_THIS);

	ch->impl->connect(host, username, passwd, dbname, port);
}

// ReSharper disable once CppVariableCanBeMadeConstexpr
ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_DESTRUCT, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseObject, __destruct)
{
	auto ch = Z_CLICKHOUSE_P(ZEND_THIS);

	delete ch->impl;
	ch->impl = nullptr;
}

ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_QUERY, 0, 0, 1)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, resultmode, IS_LONG, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseObject, query)
{
	zend_string *query = nullptr;
	zend_long resultmode = 0;

	ZEND_PARSE_PARAMETERS_START(1, 2)
		Z_PARAM_STR(query)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG(resultmode)
	ZEND_PARSE_PARAMETERS_END();

	// For compatibility with mysqli
	if (resultmode) {}

	auto obj = Z_CLICKHOUSE_P(ZEND_THIS);

	bool success = false;

	zend_object *result = obj->impl->query(string(ZSTR_VAL(query), ZSTR_LEN(query)), success);
	if (result == nullptr)
	{
		if (success)
			RETURN_TRUE;
		RETURN_FALSE;
	}

	RETVAL_OBJ(result);
}

ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_INSERT, 0, 0, 2)
	ZEND_ARG_TYPE_INFO(0, table_name, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, fields, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseObject, insert)
{
	zend_string *table_name = nullptr;
	zend_array *values = nullptr;
	zend_array *fields = nullptr;

	ZEND_PARSE_PARAMETERS_START(2, 3)
		Z_PARAM_STR(table_name)
		Z_PARAM_ARRAY_HT(values)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY_HT(fields)
	ZEND_PARSE_PARAMETERS_END();

	auto obj = Z_CLICKHOUSE_P(ZEND_THIS);

	// ReSharper disable once CppTooWideScope
	bool result = obj->impl->insert(string(ZSTR_VAL(table_name), ZSTR_LEN(table_name)), values, fields);
	if (result)
		RETURN_TRUE;
	RETURN_FALSE;
}

// ReSharper disable once CppVariableCanBeMadeConstexpr
ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_RESULT_DESTRUCT, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, __destruct)
{
	auto obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	delete obj->impl;
	obj->impl = nullptr;
}

// ReSharper disable once CppVariableCanBeMadeConstexpr
ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_RESULT_FETCH_ASSOC, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_assoc)
{
	// ReSharper disable once CppTooWideScopeInitStatement
	auto obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	if (!obj->impl->fetch_assoc(return_value))
		RETURN_FALSE;
}

// ReSharper disable once CppVariableCanBeMadeConstexpr
ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_RESULT_FETCH_ROW, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_row)
{
	// ReSharper disable once CppTooWideScopeInitStatement
	auto obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	if (!obj->impl->fetch_row(return_value))
		RETURN_FALSE;
}

// ReSharper disable once CppVariableCanBeMadeConstexpr
ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_RESULT_FETCH_ARRAY, 0, 0, 0)
	ZEND_ARG_TYPE_INFO(0, resulttype, IS_LONG, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_array)
{
	zend_long resulttype = static_cast<zend_long>(ClickHouseResult::FetchType::BOTH);

	ZEND_PARSE_PARAMETERS_START(0, 1)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG(resulttype)
	ZEND_PARSE_PARAMETERS_END();

	auto obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	// ReSharper disable once CppTooWideScopeInitStatement
	ClickHouseResult::FetchType type = ClickHouseResult::get_fetch_type(resulttype);

	if (!obj->impl->fetch_array(return_value, type))
		RETURN_FALSE;
}

// ReSharper disable once CppVariableCanBeMadeConstexpr
ZEND_BEGIN_ARG_INFO_EX(ARGINFO_CLICKHOUSE_RESULT_FETCH_ALL, 0, 0, 0)
	ZEND_ARG_TYPE_INFO(0, resulttype, IS_LONG, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_all)
{
	zend_long resulttype = static_cast<zend_long>(ClickHouseResult::FetchType::NUM);

	ZEND_PARSE_PARAMETERS_START(0, 1)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG(resulttype)
	ZEND_PARSE_PARAMETERS_END();

	auto obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	// ReSharper disable once CppTooWideScopeInitStatement
	ClickHouseResult::FetchType type = ClickHouseResult::get_fetch_type(resulttype);

	obj->impl->fetch_all(return_value, type);
}

static constexpr zend_function_entry EXTENSION_FUNCTIONS[] = {
	PHP_FE_END
};

static const zend_function_entry CLICKHOUSE_FUNCTIONS[] = {
	PHP_ME(ClickHouseObject, __construct, ARGINFO_CLICKHOUSE_CONSTRUCT, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseObject, __destruct, ARGINFO_CLICKHOUSE_DESTRUCT, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseObject, query, ARGINFO_CLICKHOUSE_QUERY, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseObject, insert, ARGINFO_CLICKHOUSE_INSERT, ZEND_ACC_PUBLIC)
	PHP_FE_END
};

static const zend_function_entry CLICKHOUSE_RESULT_FUNCTIONS[] = {
	PHP_ME(ClickHouseResultObject, __destruct, ARGINFO_CLICKHOUSE_RESULT_DESTRUCT, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_assoc, ARGINFO_CLICKHOUSE_RESULT_FETCH_ASSOC, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_row, ARGINFO_CLICKHOUSE_RESULT_FETCH_ROW, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_array, ARGINFO_CLICKHOUSE_RESULT_FETCH_ARRAY, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_all, ARGINFO_CLICKHOUSE_RESULT_FETCH_ALL, ZEND_ACC_PUBLIC)
	PHP_FE_END
};

PHP_MINIT_FUNCTION(clickhouse)
{
	// ClickHouse
	zend_class_entry ce;
	INIT_CLASS_ENTRY(ce, "ClickHouse", CLICKHOUSE_FUNCTIONS)
	clickhouse_class_entry = zend_register_internal_class(&ce);
	clickhouse_class_entry->create_object = clickhouse_new;

	memcpy(&clickhouse_object_handlers, &std_object_handlers, sizeof(zend_object_handlers));
	clickhouse_object_handlers.offset = XtOffsetOf(ClickHouseObject, std);
	clickhouse_object_handlers.free_obj = clickhouse_free;

	REGISTER_LONG_CONSTANT("CLICKHOUSE_ASSOC", static_cast<zend_long>(ClickHouseResult::FetchType::ASSOC), CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("CLICKHOUSE_NUM", static_cast<zend_long>(ClickHouseResult::FetchType::NUM), CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("CLICKHOUSE_BOTH", static_cast<zend_long>(ClickHouseResult::FetchType::BOTH), CONST_CS | CONST_PERSISTENT);

 	zend_declare_property_long(clickhouse_class_entry, "errno", sizeof("errno") - 1, 0, ZEND_ACC_PUBLIC);
	zend_declare_property_string(clickhouse_class_entry, "error", sizeof("error") - 1, "", ZEND_ACC_PUBLIC);

 	zend_declare_property_long(clickhouse_class_entry, "affected_rows", sizeof("affected_rows") - 1, 0, ZEND_ACC_PUBLIC);

	// ClickHouseResult
	zend_class_entry rce;
	INIT_CLASS_ENTRY(rce, "ClickHouseResult", CLICKHOUSE_RESULT_FUNCTIONS)
	clickhouse_result_class_entry = zend_register_internal_class(&rce);

	memcpy(&clickhouse_object_result_handlers, &std_object_handlers, sizeof(zend_object_handlers));
	clickhouse_object_result_handlers.offset = XtOffsetOf(ClickHouseResultObject, std);
	clickhouse_object_result_handlers.free_obj = clickhouse_result_free;

 	zend_declare_property_long(clickhouse_result_class_entry, "num_rows", sizeof("num_rows") - 1, 0, ZEND_ACC_PUBLIC);

	return SUCCESS;
}

PHP_RINIT_FUNCTION(clickhouse)
{
#if defined(ZTS) && defined(COMPILE_DL_CLICKHOUSE)
	ZEND_TSRMLS_CACHE_UPDATE();
#endif

	return SUCCESS;
}

PHP_MINFO_FUNCTION(clickhouse)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "ClickHouse support", "enabled");
	php_info_print_table_end();
}

zend_module_entry clickhouse_module_entry = {
	STANDARD_MODULE_HEADER,
	"clickhouse",							/* Extension name */
	EXTENSION_FUNCTIONS,							/* zend_function_entry */
	PHP_MINIT(clickhouse),					/* PHP_MINIT - Module initialization */
	nullptr,						/* PHP_MSHUTDOWN - Module shutdown */
	PHP_RINIT(clickhouse),					/* PHP_RINIT - Request initialization */
	nullptr,						/* PHP_RSHUTDOWN - Request shutdown */
	PHP_MINFO(clickhouse),							/* PHP_MINFO - Module info */
	MODULE_VERSION,								/* Version */
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_CLICKHOUSE
#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE()
#endif
ZEND_GET_MODULE(clickhouse)
#endif