#pragma once

#include "ClickHouseResult.h"

class ClickHouseDB
{
private:
	inline static const string DEFAULT_HOST = "127.0.0.1";
	inline static const string DEFAULT_USERNAME = "default";
	inline static const string DEFAULT_PASSWD;
	inline static const string DEFAULT_DBNAME = "default";
	inline static const uint32_t DEFAULT_PORT = 9000;

	zend_object *zend_this;

	shared_ptr<Client> client;

	long int timezone_offset;

	[[nodiscard]] bool is_connected() const;

	[[nodiscard]] bool do_insert(const string &table_name, zend_array *values, zend_array *fields);

	void set_error(zend_long code, const char *message) const;
	void set_affected_rows(zend_long value) const;

	template<class T, class V>
	static void add_value(Block &block, zend_string *name, zend_ulong index, V value, bool nullable, bool is_null);

	static void add_fixed_string(Block &block, zend_string *name, zend_ulong index, const string_view &value, zend_long size, bool nullable, bool is_null);

	[[nodiscard]] static bool parse_fields(zend_array *fields, vector<zend_string*> &data);

	[[nodiscard]] static bool set_column_index(zend_array *names, zend_string *name);

	[[nodiscard]] static bool add_by_type(Block &block, zend_string *name, zend_ulong index, zval *z_value, const ColumnRef &description_column, bool nullable = false);

public:
	explicit ClickHouseDB(zend_object *zend_this);

	void connect(zend_string *host, zend_string *username, zend_string *passwd, zend_string *dbname, zend_long port);

	[[nodiscard]] zend_object* query(const string &query, bool &success);
	[[nodiscard]] bool insert(const string &table_name, zend_array *values, zend_array *fields);
};

template<class T, class V>
void ClickHouseDB::add_value(Block &block, zend_string *name, zend_ulong index, V value, bool nullable, bool is_null)
{
	if (block.GetColumnCount() <= index)
	{
		if (nullable)
		{
			auto nested = make_shared<T>();
			auto nulls = make_shared<ColumnUInt8>();

			block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnNullable>(nested, nulls));
		}
		else
			block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<T>());
	}

	if (nullable)
	{
		auto column = block[index]->As<ColumnNullable>();

		column->Nested()->As<T>()->Append(value);
		column->Nulls()->As<ColumnUInt8>()->Append(is_null ? 1 : 0);
	}
	else
		block[index]->As<T>()->Append(value);
}