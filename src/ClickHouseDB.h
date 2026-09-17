#pragma once

#include "util.h"

class ClickHouseDB
{
private:
	static constexpr uint32_t DEFAULT_PORT = 9000;

	static constexpr string DEFAULT_HOST = "127.0.0.1";
	static constexpr string DEFAULT_USERNAME = "default";
	static constexpr string DEFAULT_PASSWD = "";
	static constexpr string DEFAULT_DBNAME = "default";

	zend_object *zend_this;

	shared_ptr<Client> client;

	[[nodiscard]] auto is_connected() const -> bool;

	[[nodiscard]] auto do_insert(const string &table_name, zend_array *values, zend_array *fields) const -> bool;

	void set_error(zend_long code, const char *message) const;
	void set_affected_rows(zend_long value) const;

	[[nodiscard]] static auto parse_fields(const zend_array *fields, vector<zend_string *> &data) -> bool;

	[[nodiscard]] static auto set_column_index(zend_array *names, zend_string *name) -> bool;

	[[nodiscard]] static auto fill_columns(const zend_array *values, const vector<ColumnRef> &columns, const zend_array *column_names, const vector<zend_string*> &fields_data, bool numeric_keys, zend_long &rows) -> bool;

	[[nodiscard]] static auto create_column(const TypeRef &type) -> ColumnRef;
	[[nodiscard]] static auto wrap_low_cardinality(const ColumnRef &column) -> ColumnRef;

	[[nodiscard]] static auto append_value(const ColumnRef &column, zval *value, const zend_string *name) -> bool;
	[[nodiscard]] static auto append_default(const ColumnRef &column, const zend_string *name) -> bool;
	[[nodiscard]] static auto append_map(const ColumnRef &column, const zend_array *pairs, const zend_string *name) -> bool;

	[[nodiscard]] static auto parse_date(const char *text) -> std::optional<time_t>;
	[[nodiscard]] static auto parse_datetime(const char *text) -> std::optional<time_t>;
	[[nodiscard]] static auto parse_fraction(const char *text, size_t precision) -> int64_t;
	[[nodiscard]] static auto parse_time(const char *text, size_t precision) -> std::optional<int64_t>;

	template<class T, class V>
	[[nodiscard]] static auto append_integer(const ColumnRef &column, zval *value, const zend_string *name) -> bool;

	template<class T>
	[[nodiscard]] static auto php_to_geo(zval *value, T &out, const zend_string *name) -> bool;

	[[nodiscard]] static auto type_mismatch(const zend_string *name, const char *expected) -> bool;

public:
	explicit ClickHouseDB(zend_object *zend_this);

	void connect(const zend_string *host, const zend_string *username, const zend_string *passwd, const zend_string *dbname, zend_long port);

	[[nodiscard]] auto query(const string &query, bool &success) const -> zend_object*;
	[[nodiscard]] auto insert(const string &table_name, zend_array *values, zend_array *fields) const -> bool;
};

template<class T, class V>
auto ClickHouseDB::append_integer(const ColumnRef &column, zval *value, const zend_string *name) -> bool
{
	switch (Z_TYPE_P(value))
	{
		case IS_LONG:
			column->As<T>()->Append(static_cast<V>(Z_LVAL_P(value)));
			return true;
		case IS_TRUE:
			column->As<T>()->Append(static_cast<V>(1));
			return true;
		case IS_FALSE:
			column->As<T>()->Append(static_cast<V>(0));
			return true;
		case IS_STRING:
			if constexpr (std::is_same_v<V, Int128> || std::is_same_v<V, UInt128>)
			{
				std::optional<V> number;
				if constexpr (std::is_same_v<V, Int128>)
					number = string_to_int128(string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)));
				else
					number = string_to_uint128(string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)));

				if (!number)
					return type_mismatch(name, "integer");

				column->As<T>()->Append(*number);
				return true;
			}
			return type_mismatch(name, "integer");
		default:
			return type_mismatch(name, "integer");
	}
}

template<class T>
auto ClickHouseDB::php_to_geo(zval *value, T &out, const zend_string *name) -> bool
{
	if (Z_TYPE_P(value) != IS_ARRAY)
		return type_mismatch(name, "array");

	if constexpr (std::is_same_v<T, std::tuple<double, double>>)
	{
		if (zend_hash_num_elements(Z_ARR_P(value)) != 2)
		{
			zend_error(E_WARNING, "Point for column '%s' must have 2 coordinates but %u given", ZSTR_VAL(name), zend_hash_num_elements(Z_ARR_P(value)));
			return false;
		}

		double coordinates[2] = {0., 0.};
		size_t i = 0;

		zval *coordinate;
		ZEND_HASH_FOREACH_VAL(Z_ARR_P(value), coordinate)
		{
			if (Z_TYPE_P(coordinate) == IS_DOUBLE)
				coordinates[i] = Z_DVAL_P(coordinate);
			else if (Z_TYPE_P(coordinate) == IS_LONG)
				coordinates[i] = static_cast<double>(Z_LVAL_P(coordinate));
			else
				return type_mismatch(name, "float");

			i++;
		}
		ZEND_HASH_FOREACH_END();

		out = std::make_tuple(coordinates[0], coordinates[1]);
		return true;
	}
	else
	{
		zval *element;
		ZEND_HASH_FOREACH_VAL(Z_ARR_P(value), element)
		{
			typename T::value_type item;

			if (!php_to_geo(element, item, name))
				return false;

			out.push_back(std::move(item));
		}
		ZEND_HASH_FOREACH_END();

		return true;
	}
}