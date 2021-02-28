#pragma once

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
	zend_object *zend_this;

	deque<Block> blocks;

	size_t next_row;

	[[nodiscard]] bool fetch(zval *row, FetchType type);

	[[nodiscard]] bool add_type(zval *row, const ColumnRef &column, const string &name) const;

	template<class T>
	void add_long(zval *row, const ColumnRef &column, const string &name) const;

	template<class T>
	void add_float(zval *row, const ColumnRef &column, const string &name) const;

	template<class T>
	void add_string(zval *row, const ColumnRef &column, const string &name) const;

	template<class T>
	void add_date(zval *row, const ColumnRef &column, const string &name) const;

	[[nodiscard]] bool add_null(zval *row, const ColumnRef &column, const string &name) const;

	void add_decimal(zval *row, const ColumnRef &column, const string &name) const;

	void set_num_rows(zend_long value) const;

public:
	ClickHouseResult(zend_object *zend_this, deque<Block> blocks, size_t rows_count);

	[[nodiscard]] bool fetch_assoc(zval *row);
	[[nodiscard]] bool fetch_row(zval *row);
	[[nodiscard]] bool fetch_array(zval *row, FetchType type);
	[[nodiscard]] bool fetch_all(zval *rows, FetchType type);

	[[nodiscard]] static FetchType get_fetch_type(zend_long resulttype);
};

struct ClickHouseResultObject
{
	ClickHouseResult *impl;
	zend_object std;
};

template<class T>
void ClickHouseResult::add_long(zval *row, const ColumnRef &column, const string &name) const
{
	T value = column->As<ColumnVector<T>>()->At(this->next_row);

	if (!name.empty())
		add_assoc_long_ex(row, name.c_str(), name.length(), value);
	else
		add_next_index_long(row, value);
}

template<class T>
void ClickHouseResult::add_float(zval *row, const ColumnRef &column, const string &name) const
{
	T value = column->As<ColumnVector<T>>()->At(this->next_row);

	if (!name.empty())
		add_assoc_double_ex(row, name.c_str(), name.length(), value);
	else
		add_next_index_double(row, value);
}


template<class T>
void ClickHouseResult::add_string(zval *row, const ColumnRef &column, const string &name) const
{
	string_view value = column->As<T>()->At(this->next_row);

	if (!name.empty())
		add_assoc_stringl_ex(row, name.c_str(), name.length(), value.data(), value.length());
	else
		add_next_index_stringl(row, value.data(), value.length());
}

template<class T>
void ClickHouseResult::add_date(zval *row, const ColumnRef &column, const string &name) const
{
	time_t value = column->As<T>()->At(this->next_row);

	tm tm_time{};
	localtime_r(&value, &tm_time);

	char buffer[20];		//2020-01-01 00:00:00 + \0
	size_t writed = strftime(buffer, sizeof(buffer), std::is_same<T, ColumnDate>{} ? DATE_FORMAT : DATETIME_FORMAT, &tm_time);
	if (writed == 0)
		zend_error_noreturn(E_ERROR, "Failed to format DateTime to string");

	if (!name.empty())
		add_assoc_stringl_ex(row, name.c_str(), name.length(), buffer, writed);
	else
		add_next_index_stringl(row, buffer, writed);
}