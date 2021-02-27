#include "ClickHouseResult.h"

ClickHouseResult::ClickHouseResult(blocks_t &blocks):
	blocks(move(blocks)), next_row(0)
{}

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

bool ClickHouseResult::fetch(zval *row, FetchType type)
{
	while (true)
	{
		if (this->blocks.empty())
			return false;

		Block &block = this->blocks.front();

		size_t columns = block.GetColumnCount();
		size_t rows = block.GetRowCount();

		if (rows == 0)
		{
			this->blocks.pop_front();
			continue;
		}

		array_init(row);

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
			zend_error_noreturn(E_WARNING, "Unknown column type %d", type_code);
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