#include "ClickHouseResult.h"

ClickHouseResult::ClickHouseResult(zend_object *zend_this, deque<Block> blocks, size_t rows_count, long int timezone_offset):
	zend_this(zend_this), blocks(std::move(blocks)), next_row(0), timezone_offset(timezone_offset)
{
	this->set_num_rows(rows_count);
}

auto ClickHouseResult::fetch_assoc(zval *row) -> bool
{
	return this->fetch(row, FetchType::ASSOC);
}

auto ClickHouseResult::fetch_row(zval *row) -> bool
{
	return this->fetch(row, FetchType::NUM);
}

auto ClickHouseResult::fetch_array(zval *row, FetchType type) -> bool
{
	return this->fetch(row, type);
}

auto ClickHouseResult::fetch_all(zval *rows, FetchType type) -> bool
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

auto ClickHouseResult::fetch(zval *row, FetchType type) -> bool
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
					if (!this->add_type(row, block[i], block.GetColumnName(i)))
						return false;
					break;
				case FetchType::NUM:
					if (!this->add_type(row, block[i], ""))
						return false;
					break;
				case FetchType::BOTH:
					if (!this->add_type(row, block[i], ""))
						return false;
					if (!this->add_type(row, block[i], block.GetColumnName(i)))
						return false;
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

auto ClickHouseResult::add_type(zval *row, const ColumnRef &column, const string &name) const -> bool
{
	// ReSharper disable once CppTooWideScope
	Type::Code type_code = column->Type()->GetCode();

	switch (type_code)
	{
//		case Type::Code::Void:
		case Type::Code::Int8:
			this->add_long<ColumnInt8>(row, column, name);
			break;
		case Type::Code::Int16:
			this->add_long<ColumnInt16>(row, column, name);
			break;
		case Type::Code::Int32:
			this->add_long<ColumnInt32>(row, column, name);
			break;
		case Type::Code::Int64:
			this->add_long<ColumnInt64>(row, column, name);
			break;
		case Type::Code::UInt8:
			this->add_long<ColumnUInt8>(row, column, name);
			break;
		case Type::Code::UInt16:
			this->add_long<ColumnUInt16>(row, column, name);
			break;
		case Type::Code::UInt32:
			this->add_long<ColumnUInt32>(row, column, name);
			break;
		case Type::Code::UInt64:
			this->add_long<ColumnUInt64>(row, column, name);
			break;
		case Type::Code::Float32:
			this->add_float<ColumnFloat32>(row, column, name);
			break;
		case Type::Code::Float64:
			this->add_float<ColumnFloat64>(row, column, name);
			break;
		case Type::Code::String:
			this->add_string<ColumnString>(row, column, name);
			break;
		case Type::Code::FixedString:
			this->add_string<ColumnFixedString>(row, column, name);
			break;
		case Type::Code::DateTime:
			this->add_date<ColumnDateTime>(row, column, name);
			break;
		case Type::Code::DateTime64:
			this->add_date<ColumnDateTime64>(row, column, name);
			break;
		case Type::Code::Date:
			this->add_date<ColumnDate>(row, column, name);
			break;
		case Type::Code::Date32:
			this->add_date<ColumnDate32>(row, column, name);
			break;
//		case Type::Code::Array:
		case Type::Code::Nullable:
			return this->add_null(row, column, name);
//		case Type::Code::Tuple:
//		case Type::Code::Enum8:
//		case Type::Code::Enum16:
		case Type::Code::UUID:
			this->add_string<ColumnUUID>(row, column, name);
			break;
//		case Type::Code::IPv4:
//		case Type::Code::IPv6:
		case Type::Code::Int128:
			// clickhouse-cpp doesn't support it yet
			this->add_long<ColumnInt128>(row, column, name);
			break;
		case Type::Code::Decimal:
		case Type::Code::Decimal32:
		case Type::Code::Decimal64:
		case Type::Code::Decimal128:
		{
			this->add_decimal(row, column, name);
			break;
		}
		case Type::Code::LowCardinality:
		{
			auto nested_type = column->As<ColumnLowCardinality>()->GetNestedType();

			// ReSharper disable once CppTooWideScope
			Type::Code nested_code = nested_type->GetCode();

			switch (nested_code)
			{
				case Type::Code::String:
					this->add_string<ColumnLowCardinalityT<ColumnString>>(row, column, name);
					break;
				default:
					zend_error(E_WARNING, "Type LowCardinality(%s) (%d) is unsupported", nested_type->GetName().c_str(), nested_code);
					return false;
			}
			break;
		}
		default:
			zend_error(E_WARNING, "Type %s (%d) is unsupported", column->Type()->GetName().c_str(), type_code);
			return false;
	}

	return true;
}

auto ClickHouseResult::add_null(zval *row, const ColumnRef &column, const string &name) const -> bool
{
	auto value = column->As<ColumnNullable>();

	if (value->IsNull(this->next_row))
	{
		if (!name.empty())
			add_assoc_null_ex(row, name.c_str(), name.length());
		else
			add_next_index_null(row);
		return true;
	}

	return this->add_type(row, value->Nested(), name);
}

void ClickHouseResult::add_decimal(zval *row, const ColumnRef &column, const string &name) const
{
	auto decimal_column = column->As<ColumnDecimal>();

	string text_value = std::to_string(decimal_column->At(this->next_row));

	auto type_decimal = reinterpret_cast<DecimalType*>(decimal_column->Type().get());

	// ReSharper disable once CppTooWideScopeInitStatement
	size_t scale = type_decimal->GetScale();
	if (scale != 0)
		text_value.insert(text_value.length() - scale, ".");

	if (!name.empty())
		add_assoc_stringl_ex(row, name.c_str(), name.length(), text_value.data(), text_value.length());
	else
		add_next_index_stringl(row, text_value.data(), text_value.length());

//	php_printf("value = %lu, scale = %ld, text_value = '%s', text_value length = %lu\n", static_cast<uint64_t>(value), scale, text_value.c_str(), text_value.length());
}

auto ClickHouseResult::get_fetch_type(zend_long resulttype) -> FetchType
{
	// ReSharper disable once CppTooWideScope
	auto type = static_cast<FetchType>(resulttype);

	switch (type)
	{
		case FetchType::ASSOC:
		case FetchType::NUM:
		case FetchType::BOTH:
			return type;
	}

	zend_error(E_WARNING, "Unknown fetch type %lu, CLICKHOUSE_ASSOC, CLICKHOUSE_NUM or CLICKHOUSE_BOTH are supported", resulttype);
	return FetchType::BOTH;
}

void ClickHouseResult::set_num_rows(zend_long value) const
{
#if PHP_API_VERSION >= 20200930
	zend_update_property_long(this->zend_this->ce, this->zend_this, "num_rows", sizeof("num_rows") - 1, value);
#else
	zval zv;
	ZVAL_OBJ(&zv, this->zend_this);

	zend_update_property_long(this->zend_this->ce, &zv, "num_rows", sizeof("num_rows") - 1, value);
#endif
}