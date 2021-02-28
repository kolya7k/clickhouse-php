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

	void add_type(zval *row, const ColumnRef &column, const string &name) const;

	template<class T>
	void add_long(zval *row, const ColumnRef &column, const string &name) const;

	template<class T>
	void add_float(zval *row, const ColumnRef &column, const string &name) const;

	void add_string(zval *row, const ColumnRef &column, const string &name) const;
	void add_fixed_string(zval *row, const ColumnRef &column, const string &name) const;
	void add_datetime(zval *row, const ColumnRef &column, const string &name) const;
	void add_date(zval *row, const ColumnRef &column, const string &name) const;
	void add_null(zval *row, const ColumnRef &column, const string &name) const;

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