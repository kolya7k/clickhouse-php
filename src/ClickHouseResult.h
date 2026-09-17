#pragma once

#include "util.h"

#include <netinet/in.h>

class ClickHouseResult
{
	friend class ClickHouse;

public:
	enum class FetchType : uint8_t
	{
		ASSOC = 1,
		NUM = 2,
		BOTH = 3
	};

private:
	static constexpr int64_t PHP_INT_MAX = 9223372036854775807L;
	static constexpr int64_t PHP_INT_MIN = ~PHP_INT_MAX;

	zend_object *zend_this;

	deque<Block> blocks;

	size_t next_row;

	[[nodiscard]] auto fetch(zval *row, FetchType type) -> bool;

	[[nodiscard]] auto add_column(zval *row, const ColumnRef &column, const string &name) const -> bool;

	[[nodiscard]] auto to_zval(zval *value, const ColumnRef &column, size_t index) const -> bool;
	[[nodiscard]] auto item_to_zval(zval *value, const ItemView &item, const TypeRef &type) const -> bool;

	template<class T>
	void set_long(zval *value, const ColumnRef &column, size_t index) const;

	template<class T>
	void set_float(zval *value, const ColumnRef &column, size_t index) const;

	template<class T>
	void set_string(zval *value, const ColumnRef &column, size_t index) const;

	template<class T>
	void set_date(zval *value, const ColumnRef &column, size_t index) const;

	template<class T>
	void set_enum(zval *value, const ColumnRef &column, size_t index) const;

	[[nodiscard]] auto set_array(zval *value, const ColumnRef &elements) const -> bool;
	[[nodiscard]] auto set_tuple(zval *value, const ColumnRef &column, size_t index) const -> bool;
	[[nodiscard]] auto set_map(zval *value, const ColumnRef &column, size_t index) const -> bool;

	template<class T>
	void set_geo(zval *value, const T &data) const;

	template<class V>
	static void set_long_value(zval *value, V number);

	static void set_date_value(zval *value, time_t timestamp, bool with_time);
	static void set_datetime64_value(zval *value, int64_t ticks, size_t precision);
	static void set_decimal_value(zval *value, Int128 number, size_t scale);
	static void set_ipv4_value(zval *value, in_addr address);
	static void set_ipv6_value(zval *value, const in6_addr &address);

	void set_num_rows(zend_long value) const;

public:
	ClickHouseResult(zend_object *zend_this, deque<Block> blocks, size_t rows_count);

	[[nodiscard]] auto fetch_assoc(zval *row) -> bool;
	[[nodiscard]] auto fetch_row(zval *row) -> bool;
	[[nodiscard]] auto fetch_array(zval *row, FetchType type) -> bool;
	void fetch_all(zval *rows, FetchType type);

	[[nodiscard]] static auto get_fetch_type(zend_long resulttype) -> FetchType;
};

struct ClickHouseResultObject
{
	ClickHouseResult *impl;
	zend_object std;
};

template<class T>
void ClickHouseResult::set_long(zval *value, const ColumnRef &column, size_t index) const
{
	set_long_value(value, column->As<T>()->At(index));
}

template<class T>
void ClickHouseResult::set_float(zval *value, const ColumnRef &column, size_t index) const
{
	ZVAL_DOUBLE(value, column->As<T>()->At(index));
}

template<class T>
void ClickHouseResult::set_string(zval *value, const ColumnRef &column, size_t index) const
{
	auto result = column->As<T>()->At(index);
	string_view text;
	string tmp_string;

	if constexpr (std::is_same_v<std::decay_t<decltype(result)>, UUID>)
	{
		tmp_string = uuid_to_string(result);
		text = tmp_string;
	}
	else if constexpr (std::is_same_v<std::decay_t<decltype(result)>, in_addr> || std::is_same_v<std::decay_t<decltype(result)>, in6_addr>)
	{
		tmp_string = column->As<T>()->AsString(index);
		text = tmp_string;
	}
	else
		text = result;

	ZVAL_STRINGL(value, text.data(), text.length());
}

template<class T>
void ClickHouseResult::set_date(zval *value, const ColumnRef &column, size_t index) const
{
	set_date_value(value, column->As<T>()->At(index), std::is_same_v<T, ColumnDateTime>);
}

template<class T>
void ClickHouseResult::set_enum(zval *value, const ColumnRef &column, size_t index) const
{
	string_view name = column->As<T>()->NameAt(index);

	ZVAL_STRINGL(value, name.data(), name.length());
}

template<class T>
void ClickHouseResult::set_geo(zval *value, const T &data) const
{
	if constexpr (std::is_same_v<T, std::tuple<double, double>>)
	{
		array_init_size(value, 2);
		add_next_index_double(value, std::get<0>(data));
		add_next_index_double(value, std::get<1>(data));
	}
	else
	{
		array_init(value);

		for (const auto &element : data)
		{
			zval item;

			this->set_geo(&item, element);
			add_next_index_zval(value, &item);
		}
	}
}

template<class V>
void ClickHouseResult::set_long_value(zval *value, V number)
{
	bool overflow;

	if constexpr (std::is_same_v<V, UInt128>)
		overflow = number > UInt128(static_cast<uint64_t>(PHP_INT_MAX));
	else if constexpr (std::is_same_v<V, Int128>)
		overflow = number > Int128(PHP_INT_MAX) || number < Int128(PHP_INT_MIN);
	else if constexpr (std::is_unsigned_v<V>)
		overflow = number > static_cast<uint64_t>(PHP_INT_MAX);
	else
		overflow = number > PHP_INT_MAX || number < PHP_INT_MIN;

	if (overflow)
	{
		string text = std::to_string(number);

		ZVAL_STRINGL(value, text.data(), text.length());
		return;
	}

	if constexpr (std::is_same_v<V, UInt128>)
		ZVAL_LONG(value, static_cast<zend_long>(Bignum::UInt128Low64(number)));
	else if constexpr (std::is_same_v<V, Int128>)
		ZVAL_LONG(value, static_cast<zend_long>(Bignum::Int128Low64(number)));
	else
		ZVAL_LONG(value, static_cast<zend_long>(number));
}