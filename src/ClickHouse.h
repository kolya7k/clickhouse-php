#pragma once

#include "ClickHouseResult.h"

class ClickHouse
{
private:
	inline static const string DEFAULT_HOST = "127.0.0.1";
	inline static const string DEFAULT_USERNAME = "default";
	inline static const string DEFAULT_PASSWD;
	inline static const string DEFAULT_DBNAME = "default";
	inline static const uint32_t DEFAULT_PORT = 9000;

	zend_class_entry *zend_ce;

#if PHP_API_VERSION >= 20200930
	zend_object *zend_this;
#else
	zval *zend_this;
#endif

	shared_ptr<Client> client;

	[[nodiscard]] bool is_connected() const;

	void set_error(zend_long code, const char *message) const;
	void set_affected_rows(zend_long value) const;
	void set_num_rows(zend_long value) const;

	static const unordered_map<string, Type::Code> types_names;

	static bool parse_types(zend_array *types, vector<Type::Code> &data);
	static bool parse_fields(zend_array *fields, vector<zend_string*> &data);

	static Type::Code get_type(zend_array *types, zend_string *type);
	static zend_ulong get_column_index(zend_array *names, zend_string *name);

	template<class T>
	static void add_long(Block &block, zend_string *name, zend_ulong index, zend_long value);

	template<class T>
	static void add_float(Block &block, zend_string *name, zend_ulong index, double value);

	template<class T>
	static void add_null(Block &block, zend_string *name, zend_ulong index);

	static void add_string(Block &block, zend_string *name, zend_ulong index, const string_view &value);
	static void add_fixed_string(Block &block, zend_string *name, zend_ulong index, const string_view &value);

	static void add_date(Block &block, zend_string *name, zend_ulong index, time_t time);
	static void add_datetime(Block &block, zend_string *name, zend_ulong index, time_t time);

public:
	string host = DEFAULT_HOST;
	string username = DEFAULT_USERNAME;
	string passwd = DEFAULT_PASSWD;
	string dbname = DEFAULT_DBNAME;
	uint32_t port = DEFAULT_PORT;

	ClickHouse(zval *zend_this, zend_string *host, zend_string *username, zend_string *passwd, zend_string *dbname, zend_long port);

	void connect();

	ClickHouseResult* query(const string &query, bool &success);
	bool insert(const string &table_name, zend_array *values, zend_array *types, zend_array *fields);
};

template<class T>
void ClickHouse::add_long(Block &block, zend_string *name, zend_ulong index, zend_long value)
{
	if (block.GetColumnCount() <= index)
		block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<T>());

	block[index]->As<T>()->Append(value);
}

template<class T>
void ClickHouse::add_float(Block &block, zend_string *name, zend_ulong index, double value)
{
	if (block.GetColumnCount() <= index)
		block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<T>());

	block[index]->As<T>()->Append(value);
}


template<class T>
void ClickHouse::add_null(Block &block, zend_string *name, zend_ulong index)
{
	if (block.GetColumnCount() <= index)
	{
		auto nested = make_shared<T>();
		auto nulls = make_shared<ColumnUInt8>();

		block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnNullable>(nested, nulls));
	}

	block[index]->As<ColumnNullable>()->Append(true);
}