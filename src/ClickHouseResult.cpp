#include "ClickHouseResult.h"

ClickHouseResult::ClickHouseResult(zend_object *zend_this, deque<Block> blocks, size_t rows_count):
	zend_this(zend_this), blocks(move(blocks)), next_row(0)
{
	this->set_num_rows(rows_count);
}

bool ClickHouseResult::fetch_assoc(zval *row)
{
	return this->fetch(row, FetchType::ASSOC);
}

bool ClickHouseResult::fetch_row(zval *row)
{
	return this->fetch(row, FetchType::NUM);
}

bool ClickHouseResult::fetch_array(zval *row, FetchType type)
{
	return this->fetch(row, type);
}

bool ClickHouseResult::fetch_all(zval *rows, FetchType type)
{
	bool has_rows = false;
	while (true)
	{
		zval row;

		if (!this->fetch_array(&row, type))
			break;

		if (!has_rows)
		{
			has_rows = true;
			array_init(rows);
		}

		add_next_index_zval(rows, &row);
	}

	return has_rows;
}

bool ClickHouseResult::fetch(zval *row, FetchType type)
{
	while (true)
	{
		if (this->blocks.empty())
			return false;

		Block &block = this->blocks.front();

		size_t columns = block.GetColumnCount();
		size_t rows = block.GetRowCount();

		array_init_size(row, type == FetchType::BOTH ? columns * 2 : columns);

		for (size_t i = 0; i < columns; i++)
		{
			switch (type)
			{
				case FetchType::ASSOC:
					this->add_type(row, block[i], block.GetColumnName(i));
					break;
				case FetchType::NUM:
					this->add_type(row, block[i], "");
					break;
				case FetchType::BOTH:
					this->add_type(row, block[i], "");
					this->add_type(row, block[i], block.GetColumnName(i));
					break;
			}
		}

		this->next_row++;

		if (rows == this->next_row)
		{
			this->blocks.pop_front();
			this->next_row = 0;
		}

		return true;
	}
}

void ClickHouseResult::add_type(zval *row, const ColumnRef &column, const string &name) const
{
	Type::Code type_code = column->Type()->GetCode();

	switch (type_code)
	{
//		case Type::Code::Void:
		case Type::Code::Int8:
			this->add_long<int8_t>(row, column, name);
			break;
		case Type::Code::Int16:
			this->add_long<int16_t>(row, column, name);
			break;
		case Type::Code::Int32:
			this->add_long<int32_t>(row, column, name);
			break;
		case Type::Code::Int64:
			this->add_long<int64_t>(row, column, name);
			break;
		case Type::Code::UInt8:
			this->add_long<uint8_t>(row, column, name);
			break;
		case Type::Code::UInt16:
			this->add_long<uint16_t>(row, column, name);
			break;
		case Type::Code::UInt32:
			this->add_long<uint32_t>(row, column, name);
			break;
		case Type::Code::UInt64:
			this->add_long<uint64_t>(row, column, name);
			break;
		case Type::Code::Float32:
			this->add_float<float>(row, column, name);
			break;
		case Type::Code::Float64:
			this->add_float<double>(row, column, name);
			break;
		case Type::Code::String:
			this->add_string(row, column, name);
			break;
		case Type::Code::FixedString:
			this->add_fixed_string(row, column, name);
			break;
		case Type::Code::DateTime:
			this->add_datetime(row, column, name);
			break;
		case Type::Code::Date:
			this->add_date(row, column, name);
			break;
//		case Type::Code::Array:
		case Type::Code::Nullable:
			this->add_null(row, column, name);
			break;
//		case Type::Code::Tuple:
//		case Type::Code::Enum8:
//		case Type::Code::Enum16:
//		case Type::Code::UUID:
//		case Type::Code::IPv4:
//		case Type::Code::IPv6:
//		case Type::Code::Int128:
//		case Type::Code::Decimal:
//		case Type::Code::Decimal32:
//		case Type::Code::Decimal64:
//		case Type::Code::Decimal128:
//		case Type::Code::LowCardinality:
		default:
			zend_error_noreturn(E_WARNING, "Type %d is unsupported", type_code);
	}
}

void ClickHouseResult::add_string(zval *row, const ColumnRef &column, const string &name) const
{
	string_view value = column->As<ColumnString>()->At(this->next_row);

	if (!name.empty())
		add_assoc_stringl_ex(row, name.c_str(), name.length(), value.data(), value.length());
	else
		add_next_index_stringl(row, value.data(), value.length());
}

void ClickHouseResult::add_fixed_string(zval *row, const ColumnRef &column, const string &name) const
{
	auto fixed_string = column->As<ColumnFixedString>();
	string_view value = fixed_string->At(this->next_row);

	if (!name.empty())
		add_assoc_stringl_ex(row, name.c_str(), name.length(), value.data(), fixed_string->FixedSize());
	else
		add_next_index_stringl(row, value.data(), fixed_string->FixedSize());
}

void ClickHouseResult::add_datetime(zval *row, const ColumnRef &column, const string &name) const
{
	time_t value = static_cast<time_t>(column->As<ColumnDateTime>()->At(this->next_row));

	tm tm_time{};
	localtime_r(&value, &tm_time);

	char buffer[20];		//2020-01-01 00:00:00 + \0
	size_t writed = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_time);
	if (writed == 0)
		zend_error_noreturn(E_ERROR, "Failed to format DateTime to string");

	if (!name.empty())
		add_assoc_stringl_ex(row, name.c_str(), name.length(), buffer, writed);
	else
		add_next_index_stringl(row, buffer, writed);
}

void ClickHouseResult::add_date(zval *row, const ColumnRef &column, const string &name) const
{
	time_t value = column->As<ColumnDate>()->At(this->next_row);

	tm tm_time{};
	localtime_r(&value, &tm_time);

	char buffer[11];		//2020-01-01 + \0
	size_t writed = strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tm_time);
	if (writed == 0)
		zend_error_noreturn(E_ERROR, "Failed to format DateTime to string");

	if (!name.empty())
		add_assoc_stringl_ex(row, name.c_str(), name.length(), buffer, writed);
	else
		add_next_index_stringl(row, buffer, writed);
}

void ClickHouseResult::add_null(zval *row, const ColumnRef &column, const string &name) const
{
	auto value = column->As<ColumnNullable>();

	if (value->IsNull(this->next_row))
	{
		if (!name.empty())
			add_assoc_null_ex(row, name.c_str(), name.length());
		else
			add_next_index_null(row);
		return;
	}

	this->add_type(row, value->Nested(), name);
}

ClickHouseResult::FetchType ClickHouseResult::get_fetch_type(zend_long resulttype)
{
	FetchType type = static_cast<ClickHouseResult::FetchType>(resulttype);
	switch (type)
	{
		case ClickHouseResult::FetchType::ASSOC:
		case ClickHouseResult::FetchType::NUM:
		case ClickHouseResult::FetchType::BOTH:
			return type;
	}

	zend_error(E_WARNING, "Unknown fetch type %lu, CLICKHOUSE_ASSOC, CLICKHOUSE_NUM or CLICKHOUSE_BOTH are supported", resulttype);
	return ClickHouseResult::FetchType::BOTH;
}

void ClickHouseResult::set_num_rows(zend_long value) const
{
#if PHP_API_VERSION >= 20200930
	zend_update_property_long(zend_this->ce, zend_this, "num_rows", sizeof("num_rows") - 1, value);
#else
	zval zv;
	ZVAL_OBJ(&zv, zend_this);

	zend_update_property_long(zend_this->ce, &zv, "num_rows", sizeof("num_rows") - 1, value);
#endif
}