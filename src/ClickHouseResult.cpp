#include "ClickHouseResult.h"

#include <arpa/inet.h>

ClickHouseResult::ClickHouseResult(zend_object *zend_this, deque<Block> blocks, size_t rows_count):
	zend_this(zend_this), blocks(std::move(blocks)), next_row(0)
{
	this->set_num_rows(rows_count);
}

auto ClickHouseResult::fetch(zval *row, FetchType type) -> bool
{
	if (this->blocks.empty())
		return false;

	Block &block = this->blocks.front();

	size_t columns = block.GetColumnCount();
	size_t rows = block.GetRowCount();

	array_init_size(row, type == FetchType::BOTH ? columns * 2 : columns);

	for (size_t i = 0; i < columns; i++)
	{
		bool success = true;

		switch (type)
		{
			case FetchType::ASSOC:
				success = this->add_column(row, block[i], block.GetColumnName(i));
				break;
			case FetchType::NUM:
				success = this->add_column(row, block[i], "");
				break;
			case FetchType::BOTH:
				success = this->add_column(row, block[i], "") && this->add_column(row, block[i], block.GetColumnName(i));
				break;
		}

		if (!success)
		{
			zval_ptr_dtor(row);
			return false;
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

auto ClickHouseResult::add_column(zval *row, const ColumnRef &column, const string &name) const -> bool
{
	zval value;

	if (!this->to_zval(&value, column, this->next_row))
		return false;

	if (!name.empty())
		add_assoc_zval_ex(row, name.c_str(), name.length(), &value);
	else
		add_next_index_zval(row, &value);

	return true;
}

auto ClickHouseResult::to_zval(zval *value, const ColumnRef &column, size_t index) const -> bool
{
	// ReSharper disable once CppTooWideScope
	Type::Code type_code = column->Type()->GetCode();

	switch (type_code)
	{
		case Type::Code::Void:
			ZVAL_NULL(value);
			break;
		case Type::Code::Int8:
			this->set_long<ColumnInt8>(value, column, index);
			break;
		case Type::Code::Int16:
			this->set_long<ColumnInt16>(value, column, index);
			break;
		case Type::Code::Int32:
			this->set_long<ColumnInt32>(value, column, index);
			break;
		case Type::Code::Int64:
			this->set_long<ColumnInt64>(value, column, index);
			break;
		case Type::Code::Int128:
			this->set_long<ColumnInt128>(value, column, index);
			break;
		case Type::Code::UInt8:
			this->set_long<ColumnUInt8>(value, column, index);
			break;
		case Type::Code::UInt16:
			this->set_long<ColumnUInt16>(value, column, index);
			break;
		case Type::Code::UInt32:
			this->set_long<ColumnUInt32>(value, column, index);
			break;
		case Type::Code::UInt64:
			this->set_long<ColumnUInt64>(value, column, index);
			break;
		case Type::Code::UInt128:
			this->set_long<ColumnUInt128>(value, column, index);
			break;
		case Type::Code::Float32:
			this->set_float<ColumnFloat32>(value, column, index);
			break;
		case Type::Code::Float64:
			this->set_float<ColumnFloat64>(value, column, index);
			break;
		case Type::Code::String:
			this->set_string<ColumnString>(value, column, index);
			break;
		case Type::Code::FixedString:
			this->set_string<ColumnFixedString>(value, column, index);
			break;
		case Type::Code::DateTime:
			this->set_date<ColumnDateTime>(value, column, index);
			break;
		case Type::Code::DateTime64:
		{
			auto datetime = column->As<ColumnDateTime64>();

			set_datetime64_value(value, datetime->At(index), datetime->GetPrecision());
			break;
		}
		case Type::Code::Date:
			this->set_date<ColumnDate>(value, column, index);
			break;
		case Type::Code::Date32:
			this->set_date<ColumnDate32>(value, column, index);
			break;
		case Type::Code::Nullable:
		{
			auto nullable = column->As<ColumnNullable>();

			if (nullable->IsNull(index))
			{
				ZVAL_NULL(value);
				break;
			}

			return this->to_zval(value, nullable->Nested(), index);
		}
		case Type::Code::Array:
			return this->set_array(value, column->As<ColumnArray>()->GetAsColumn(index));
		case Type::Code::Tuple:
			return this->set_tuple(value, column, index);
		case Type::Code::Point:
			this->set_geo(value, column->As<ColumnPoint>()->At(index));
			break;
		case Type::Code::Ring:
			this->set_geo(value, column->As<ColumnRing>()->At(index));
			break;
		case Type::Code::Polygon:
			this->set_geo(value, column->As<ColumnPolygon>()->At(index));
			break;
		case Type::Code::MultiPolygon:
			this->set_geo(value, column->As<ColumnMultiPolygon>()->At(index));
			break;
		case Type::Code::Map:
			return this->set_map(value, column, index);
		case Type::Code::Enum8:
			this->set_enum<ColumnEnum8>(value, column, index);
			break;
		case Type::Code::Enum16:
			this->set_enum<ColumnEnum16>(value, column, index);
			break;
		case Type::Code::UUID:
			this->set_string<ColumnUUID>(value, column, index);
			break;
		case Type::Code::IPv4:
			this->set_string<ColumnIPv4>(value, column, index);
			break;
		case Type::Code::IPv6:
			this->set_string<ColumnIPv6>(value, column, index);
			break;
		case Type::Code::Decimal:
		case Type::Code::Decimal32:
		case Type::Code::Decimal64:
		case Type::Code::Decimal128:
		{
			auto decimal = column->As<ColumnDecimal>();

			set_decimal_value(value, decimal->At(index), decimal->GetScale());
			break;
		}
		case Type::Code::LowCardinality:
			return this->item_to_zval(value, column->GetItem(index), column->As<ColumnLowCardinality>()->GetNestedType());
		default:
			zend_error(E_WARNING, "Type %s (%d) is unsupported", column->Type()->GetName().c_str(), type_code);
			return false;
	}

	return true;
}

auto ClickHouseResult::item_to_zval(zval *value, const ItemView &item, const TypeRef &type) const -> bool
{
	if (item.type == Type::Code::Void)
	{
		ZVAL_NULL(value);
		return true;
	}

	TypeRef item_type = type;
	if (item_type->GetCode() == Type::Code::Nullable)
		item_type = item_type->As<NullableType>()->GetNestedType();

	switch (item.type)
	{
		case Type::Code::Int8:
			set_long_value(value, item.get<int8_t>());
			break;
		case Type::Code::Int16:
			set_long_value(value, item.get<int16_t>());
			break;
		case Type::Code::Int32:
			set_long_value(value, item.get<int32_t>());
			break;
		case Type::Code::Int64:
			set_long_value(value, item.get<int64_t>());
			break;
		case Type::Code::Int128:
			set_long_value(value, item.get<Int128>());
			break;
		case Type::Code::UInt8:
			set_long_value(value, item.get<uint8_t>());
			break;
		case Type::Code::UInt16:
			set_long_value(value, item.get<uint16_t>());
			break;
		case Type::Code::UInt32:
			set_long_value(value, item.get<uint32_t>());
			break;
		case Type::Code::UInt64:
			set_long_value(value, item.get<uint64_t>());
			break;
		case Type::Code::UInt128:
			set_long_value(value, item.get<UInt128>());
			break;
		case Type::Code::Float32:
			ZVAL_DOUBLE(value, item.get<float>());
			break;
		case Type::Code::Float64:
			ZVAL_DOUBLE(value, item.get<double>());
			break;
		case Type::Code::String:
		case Type::Code::FixedString:
		{
			string_view text = item.AsBinaryData();

			ZVAL_STRINGL(value, text.data(), text.length());
			break;
		}
		case Type::Code::Date:
			set_date_value(value, static_cast<time_t>(item.get<uint16_t>()) * 86400, false);
			break;
		case Type::Code::Date32:
			set_date_value(value, static_cast<time_t>(item.get<int32_t>()) * 86400, false);
			break;
		case Type::Code::DateTime:
			set_date_value(value, item.get<uint32_t>(), true);
			break;
		case Type::Code::DateTime64:
			set_datetime64_value(value, item.get<int64_t>(), item_type->As<DateTime64Type>()->GetPrecision());
			break;
		case Type::Code::Decimal:
		case Type::Code::Decimal32:
		case Type::Code::Decimal64:
		case Type::Code::Decimal128:
		{
			string_view data = item.AsBinaryData();
			Int128 number;

			if (data.size() == sizeof(int32_t))
				number = item.get<int32_t>();
			else if (data.size() == sizeof(int64_t))
				number = item.get<int64_t>();
			else
				number = item.get<Int128>();

			set_decimal_value(value, number, item_type->As<DecimalType>()->GetScale());
			break;
		}
		case Type::Code::Enum8:
		case Type::Code::Enum16:
		{
			int16_t number = item.type == Type::Code::Enum8 ? item.get<int8_t>() : item.get<int16_t>();
			string_view name = item_type->As<EnumType>()->GetEnumName(number);

			ZVAL_STRINGL(value, name.data(), name.length());
			break;
		}
		case Type::Code::IPv4:
		{
			in_addr address{};
			address.s_addr = item.get<uint32_t>();

			set_ipv4_value(value, address);
			break;
		}
		case Type::Code::IPv6:
		{
			in6_addr address{};
			memcpy(&address, item.AsBinaryData().data(), sizeof(address));

			set_ipv6_value(value, address);
			break;
		}
		case Type::Code::UUID:
		{
			UUID uuid;
			memcpy(&uuid.first, item.AsBinaryData().data(), sizeof(uint64_t));
			memcpy(&uuid.second, item.AsBinaryData().data() + sizeof(uint64_t), sizeof(uint64_t));

			string text = std::uuid_to_string(uuid);

			ZVAL_STRINGL(value, text.data(), text.length());
			break;
		}
		default:
			zend_error(E_WARNING, "Type LowCardinality(%s) (%d) is unsupported", type->GetName().c_str(), item.type);
			return false;
	}

	return true;
}

auto ClickHouseResult::set_array(zval *value, const ColumnRef &elements) const -> bool
{
	size_t size = elements->Size();

	array_init_size(value, size);

	for (size_t i = 0; i < size; i++)
	{
		zval element;

		if (!this->to_zval(&element, elements, i))
		{
			zval_ptr_dtor(value);
			return false;
		}

		add_next_index_zval(value, &element);
	}

	return true;
}

auto ClickHouseResult::set_tuple(zval *value, const ColumnRef &column, size_t index) const -> bool
{
	auto tuple = column->As<ColumnTuple>();
	size_t size = tuple->TupleSize();

	array_init_size(value, size);

	for (size_t i = 0; i < size; i++)
	{
		zval element;

		if (!this->to_zval(&element, tuple->At(i), index))
		{
			zval_ptr_dtor(value);
			return false;
		}

		add_next_index_zval(value, &element);
	}

	return true;
}

auto ClickHouseResult::set_map(zval *value, const ColumnRef &column, size_t index) const -> bool
{
	auto pairs = column->As<ColumnMap>()->GetAsColumn(index)->As<ColumnTuple>();
	size_t size = pairs->Size();

	array_init_size(value, size);

	for (size_t i = 0; i < size; i++)
	{
		zval key;
		zval element;

		if (!this->to_zval(&key, pairs->At(0), i))
		{
			zval_ptr_dtor(value);
			return false;
		}

		if (!this->to_zval(&element, pairs->At(1), i))
		{
			zval_ptr_dtor(&key);
			zval_ptr_dtor(value);
			return false;
		}

		if (Z_TYPE(key) == IS_LONG)
		{
			add_index_zval(value, Z_LVAL(key), &element);
			continue;
		}

		zend_string *key_string = zval_get_string(&key);
		zend_symtable_update(Z_ARRVAL_P(value), key_string, &element);
		zend_string_release(key_string);
		zval_ptr_dtor(&key);
	}

	return true;
}

void ClickHouseResult::set_date_value(zval *value, time_t timestamp, bool with_time)
{
	tm tm_time{};
	localtime_r(&timestamp, &tm_time);

	char buffer[20];		// 2020-01-01 00:00:00 + \0
	size_t written = strftime(buffer, sizeof(buffer), with_time ? DATETIME_FORMAT : DATE_FORMAT, &tm_time);
	if (written == 0)
		zend_error_noreturn(E_ERROR, "Failed to format DateTime to string");

	ZVAL_STRINGL(value, buffer, written);
}

void ClickHouseResult::set_datetime64_value(zval *value, int64_t ticks, size_t precision)
{
	int64_t scale = pow10_int64(precision);
	int64_t seconds = ticks / scale;
	int64_t fraction = ticks % scale;

	if (fraction < 0)
	{
		fraction += scale;
		seconds--;
	}

	tm tm_time{};
	time_t timestamp = seconds;
	localtime_r(&timestamp, &tm_time);

	char buffer[32];		// 2020-01-01 00:00:00.123456789 + \0
	size_t written = strftime(buffer, sizeof(buffer), DATETIME_FORMAT, &tm_time);
	if (written == 0)
		zend_error_noreturn(E_ERROR, "Failed to format DateTime64 to string");

	if (precision > 0)
		written += snprintf(buffer + written, sizeof(buffer) - written, ".%0*" PRId64, static_cast<int>(precision), fraction);

	ZVAL_STRINGL(value, buffer, written);
}

void ClickHouseResult::set_decimal_value(zval *value, Int128 number, size_t scale)
{
	bool negative = number < 0;
	string digits = std::to_string(negative ? Int128(0) - number : number);

	if (digits.length() <= scale)
		digits.insert(0, scale + 1 - digits.length(), '0');

	if (scale != 0)
		digits.insert(digits.length() - scale, ".");

	if (negative)
		digits.insert(digits.begin(), '-');

	ZVAL_STRINGL(value, digits.data(), digits.length());
}

void ClickHouseResult::set_ipv4_value(zval *value, in_addr address)
{
	char buffer[INET_ADDRSTRLEN];
	const char *text = inet_ntop(AF_INET, &address, buffer, sizeof(buffer));
	if (text == nullptr)
		text = "";

	ZVAL_STRING(value, text);
}

void ClickHouseResult::set_ipv6_value(zval *value, const in6_addr &address)
{
	char buffer[INET6_ADDRSTRLEN];
	const char *text = inet_ntop(AF_INET6, &address, buffer, sizeof(buffer));
	if (text == nullptr)
		text = "";

	ZVAL_STRING(value, text);
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

void ClickHouseResult::fetch_all(zval *rows, FetchType type)
{
	array_init(rows);

	while (true)
	{
		zval row;

		if (!this->fetch_array(&row, type))
			break;

		add_next_index_zval(rows, &row);
	}
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