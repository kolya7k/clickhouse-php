#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "ClickHouse.h"

static constexpr const char *MODULE_VERSION = "1.0.0";

static zend_class_entry *clickhouse_class_entry = nullptr;
static zend_class_entry *clickhouse_result_class_entry = nullptr;

static zend_object_handlers clickhouse_object_handlers;
static zend_object_handlers clickhouse_object_result_handlers;

struct ClickHouseObject
{
	ClickHouse *impl;
	zend_object std;
};

struct ClickHouseResultObject
{
	ClickHouseResult *impl;
	zend_object std;
};

#define Z_CLICKHOUSE(zv) ((ClickHouseObject*)((char*)(zv) - XtOffsetOf(ClickHouseObject, std)))
#define Z_CLICKHOUSE_P(zv) Z_CLICKHOUSE(Z_OBJ_P(zv))

#define Z_CLICKHOUSE_RESULT(zv) ((ClickHouseResultObject*)((char*)(zv) - XtOffsetOf(ClickHouseResultObject, std)))
#define Z_CLICKHOUSE_RESULT_P(zv) Z_CLICKHOUSE_RESULT(Z_OBJ_P(zv))

__inline static zend_object* clickhouse_new(zend_class_entry *ce)
{
	ClickHouseObject *obj = static_cast<ClickHouseObject*>(zend_object_alloc(sizeof(ClickHouseObject), ce));

	zend_object_std_init(&obj->std, ce);
	object_properties_init(&obj->std, ce);

	obj->std.handlers = &clickhouse_object_handlers;

	return &obj->std;
}

__inline static zend_object* clickhouse_result_new(zend_class_entry *ce, ClickHouseResult *result)
{
	ClickHouseResultObject *obj = static_cast<ClickHouseResultObject*>(zend_object_alloc(sizeof(ClickHouseResultObject), ce));

	zend_object_std_init(&obj->std, ce);
	object_properties_init(&obj->std, ce);

	obj->std.handlers = &clickhouse_object_result_handlers;

	obj->impl = result;

	return &obj->std;
}

__inline static void clickhouse_result_free(zend_object *obj)
{
	ClickHouseResultObject *ch_obj = Z_CLICKHOUSE_RESULT(obj);

	delete ch_obj->impl;
	ch_obj->impl = nullptr;
}

__inline static void clickhouse_free(zend_object *obj)
{
	ClickHouseObject *ch_obj = Z_CLICKHOUSE(obj);

	delete ch_obj->impl;
	ch_obj->impl = nullptr;
}

ZEND_DECLARE_MODULE_GLOBALS(clickhouse)

ZEND_MODULE_GLOBALS_CTOR_D(clickhouse)
{}

ZEND_MODULE_GLOBALS_DTOR_D(clickhouse)
{}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_construct, 0, 0, 0)
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

	ClickHouseObject *ch = Z_CLICKHOUSE_P(ZEND_THIS);

	ch->impl = new ClickHouse(ZEND_THIS, host, username, passwd, dbname, port);
	ch->impl->connect();
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_destruct, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseObject, __destruct)
{
	ClickHouseObject *ch = Z_CLICKHOUSE_P(ZEND_THIS);

	delete ch->impl;
	ch->impl = nullptr;
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_query, 0, 0, 1)
	ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, resultmode, IS_LONG, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseObject, query)
{
	zend_string *query;
	zend_long resultmode;

	ZEND_PARSE_PARAMETERS_START(1, 2)
		Z_PARAM_STR(query)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG(resultmode)
	ZEND_PARSE_PARAMETERS_END();

	// For compatibility with mysqli
	if (resultmode) {}

	ClickHouseObject *obj = Z_CLICKHOUSE_P(ZEND_THIS);

	bool success;
	ClickHouseResult *result = obj->impl->query(string(ZSTR_VAL(query), ZSTR_LEN(query)), success);
	if (result == nullptr)
	{
		if (success)
			RETURN_TRUE;
		RETURN_FALSE;
	}

	zend_object *result_obj = clickhouse_result_new(clickhouse_result_class_entry, result);

	RETVAL_OBJ(result_obj);
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_insert, 0, 0, 3)
	ZEND_ARG_TYPE_INFO(0, table_name, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, types, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, fields, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseObject, insert)
{
	zend_string *table_name;
	zend_array *values;
	zend_array *types;
	zend_array *fields = nullptr;

	ZEND_PARSE_PARAMETERS_START(3, 4)
		Z_PARAM_STR(table_name)
		Z_PARAM_ARRAY_HT(values)
		Z_PARAM_ARRAY_HT(types)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY_HT(fields)
	ZEND_PARSE_PARAMETERS_END();

	ClickHouseObject *obj = Z_CLICKHOUSE_P(ZEND_THIS);

	bool result = obj->impl->insert(string(ZSTR_VAL(table_name), ZSTR_LEN(table_name)), values, types, fields);
	if (result)
		RETURN_TRUE;
	RETURN_FALSE;
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_result_destruct, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, __destruct)
{
	ClickHouseResultObject *obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	delete obj->impl;
	obj->impl = nullptr;
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_result_fetch_assoc, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_assoc)
{
	ClickHouseResultObject *obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	if (!obj->impl->fetch_assoc(return_value))
		RETURN_FALSE;
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_result_fetch_row, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_row)
{
	ClickHouseResultObject *obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	if (!obj->impl->fetch_row(return_value))
		RETURN_FALSE;
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_result_fetch_array, 0, 0, 0)
	ZEND_ARG_TYPE_INFO(0, resulttype, IS_LONG, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_array)
{
	zend_long resulttype = static_cast<zend_long>(ClickHouseResult::FetchType::BOTH);

	ZEND_PARSE_PARAMETERS_START(0, 1)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG(resulttype)
	ZEND_PARSE_PARAMETERS_END();

	ClickHouseResultObject *obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	ClickHouseResult::FetchType type = ClickHouseResult::get_fetch_type(resulttype);

	if (!obj->impl->fetch_array(return_value, type))
		RETURN_FALSE;
}


ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_result_fetch_all, 0, 0, 0)
	ZEND_ARG_TYPE_INFO(0, resulttype, IS_LONG, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(ClickHouseResultObject, fetch_all)
{
	zend_long resulttype = static_cast<zend_long>(ClickHouseResult::FetchType::NUM);

	ZEND_PARSE_PARAMETERS_START(0, 1)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG(resulttype)
	ZEND_PARSE_PARAMETERS_END();

	ClickHouseResultObject *obj = Z_CLICKHOUSE_RESULT_P(ZEND_THIS);

	ClickHouseResult::FetchType type = ClickHouseResult::get_fetch_type(resulttype);

	if (!obj->impl->fetch_all(return_value, type))
		RETURN_FALSE;
}

static const zend_function_entry clickhouse_functions[] = {
	PHP_ME(ClickHouseObject, __construct, arginfo_clickhouse_construct, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseObject, __destruct, arginfo_clickhouse_destruct, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseObject, query, arginfo_clickhouse_query, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseObject, insert, arginfo_clickhouse_insert, ZEND_ACC_PUBLIC)
	PHP_FE_END
};

static const zend_function_entry clickhouse_result_functions[] = {
	PHP_ME(ClickHouseResultObject, __destruct, arginfo_clickhouse_result_destruct, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_assoc, arginfo_clickhouse_result_fetch_assoc, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_row, arginfo_clickhouse_result_fetch_row, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_array, arginfo_clickhouse_result_fetch_array, ZEND_ACC_PUBLIC)
	PHP_ME(ClickHouseResultObject, fetch_all, arginfo_clickhouse_result_fetch_all, ZEND_ACC_PUBLIC)
	PHP_FE_END
};

PHP_MINIT_FUNCTION(clickhouse)
{
	// ClickHouse
	zend_class_entry ce;
	INIT_CLASS_ENTRY(ce, "ClickHouse", clickhouse_functions);
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
 	zend_declare_property_long(clickhouse_class_entry, "num_rows", sizeof("num_rows") - 1, 0, ZEND_ACC_PUBLIC);

	// ClickHouseResult
	zend_class_entry rce;
	INIT_CLASS_ENTRY(rce, "ClickHouseResult", clickhouse_result_functions);
	clickhouse_result_class_entry = zend_register_internal_class(&rce);

	memcpy(&clickhouse_object_result_handlers, &std_object_handlers, sizeof(zend_object_handlers));
	clickhouse_object_result_handlers.offset = XtOffsetOf(ClickHouseResultObject, std);
	clickhouse_object_result_handlers.free_obj = clickhouse_result_free;

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

typedef void (*zend_ctor_type)(void*);

zend_module_entry clickhouse_module_entry = {
	STANDARD_MODULE_HEADER,
	"clickhouse",							/* Extension name */
	clickhouse_functions,							/* zend_function_entry */
	PHP_MINIT(clickhouse),							/* PHP_MINIT - Module initialization */
	nullptr,						/* PHP_MSHUTDOWN - Module shutdown */
	PHP_RINIT(clickhouse),							/* PHP_RINIT - Request initialization */
	nullptr,						/* PHP_RSHUTDOWN - Request shutdown */
	PHP_MINFO(clickhouse),							/* PHP_MINFO - Module info */
	MODULE_VERSION,								/* Version */
	ZEND_MODULE_GLOBALS(clickhouse),
	reinterpret_cast<zend_ctor_type>(ZEND_MODULE_GLOBALS_CTOR_N(clickhouse)),
	reinterpret_cast<zend_ctor_type>(ZEND_MODULE_GLOBALS_DTOR_N(clickhouse)),
	nullptr,
	STANDARD_MODULE_PROPERTIES_EX
};

#ifdef COMPILE_DL_CLICKHOUSE
#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE()
#endif
ZEND_GET_MODULE(clickhouse)
#endif