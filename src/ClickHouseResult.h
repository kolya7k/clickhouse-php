#pragma once

class ClickHouseResult
{
	friend class ClickHouse;

private:
	typedef deque<Block> blocks_t;

	blocks_t blocks;

	size_t next_row;

	[[nodiscard]] bool fetch(zval *row, bool assoc);

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

public:
	explicit ClickHouseResult(blocks_t &blocks);

	[[nodiscard]] bool fetch_assoc(zval *row);
	[[nodiscard]] bool fetch_row(zval *row);
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