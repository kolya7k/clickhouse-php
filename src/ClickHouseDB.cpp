#include "ClickHouseDB.h"

#include "ClickHouseResult.h"

#include "clickhouse/columns/factory.h"

__inline static auto clickhouse_result_new(deque<Block> blocks, zend_long rows_count) -> zend_object *
{
	auto obj = static_cast<ClickHouseResultObject*>(zend_object_alloc(sizeof(ClickHouseResultObject), clickhouse_result_class_entry));

	zend_object_std_init(&obj->std, clickhouse_result_class_entry);
	object_properties_init(&obj->std, clickhouse_result_class_entry);

	obj->std.handlers = &clickhouse_object_result_handlers;

	obj->impl = new ClickHouseResult(&obj->std, std::move(blocks), rows_count);

	return &obj->std;
}

ClickHouseDB::ClickHouseDB(zend_object *zend_this):
	zend_this(zend_this)
{}

void ClickHouseDB::connect(const zend_string *host, const zend_string *username, const zend_string *passwd, const zend_string *dbname, zend_long port)
{
	ClientOptions options;
	if (host != nullptr)
		options.SetHost(string(ZSTR_VAL(host), ZSTR_LEN(host)));
	else
		options.SetHost(DEFAULT_HOST);

	if (username != nullptr)
		options.SetUser(string(ZSTR_VAL(username), ZSTR_LEN(username)));
	else
		options.SetUser(DEFAULT_USERNAME);

	if (passwd != nullptr)
		options.SetPassword(string(ZSTR_VAL(passwd), ZSTR_LEN(passwd)));
	else
		options.SetPassword(DEFAULT_PASSWD);

	if (dbname != nullptr)
		options.SetDefaultDatabase(string(ZSTR_VAL(dbname), ZSTR_LEN(dbname)));
	else
		options.SetDefaultDatabase(DEFAULT_DBNAME);

	if (port != 0)
		options.SetPort(static_cast<uint16_t>(port));
	else
		options.SetPort(DEFAULT_PORT);

	options.SetCompressionMethod(CompressionMethod::LZ4);

	try
	{
		this->client = make_shared<Client>(options);
	}
	catch (const std::exception &e)
	{
		zend_error(E_WARNING, "Failed to connect to ClickHouse: %s", e.what());
		this->client.reset();
	}
}

auto ClickHouseDB::query(const string &query, bool &success) const -> zend_object*
{
	this->set_error(0, "");
	this->set_affected_rows(0);

	if (!this->is_connected())
		return nullptr;

	deque<Block> blocks;
	zend_long rows_count = 0;
	bool has_data = false;

	try
	{
		Query ch_query(query);
		ch_query.OnData([&blocks, &rows_count, &has_data] (const Block &block)
		{
			if (block.GetColumnCount() != 0)
				has_data = true;

			if (block.GetRowCount() == 0)
				return;

			rows_count += static_cast<zend_long>(block.GetRowCount());
			blocks.push_back(block);
		});

		this->client->Execute(ch_query);
	}
	catch (const ServerException &e)
	{
		success = false;

		this->set_error(e.GetCode(), e.what());
		this->set_affected_rows(-1);

		this->client->ResetConnection();
		return nullptr;
	}
	catch (const std::exception &e)
	{
		success = false;

		this->set_error(0, e.what());
		this->set_affected_rows(-1);

		this->client->ResetConnection();
		return nullptr;
	}

	success = true;

	if (!has_data)
		return nullptr;

	this->set_affected_rows(rows_count);

	return clickhouse_result_new(std::move(blocks), rows_count);
}

auto ClickHouseDB::insert(const string &table_name, zend_array *values, zend_array *fields) const -> bool
{
	try
	{
		return this->do_insert(table_name, values, fields);
	}
	catch (const ServerException &e)
	{
		this->set_error(e.GetCode(), e.what());
		this->set_affected_rows(-1);

		this->client->ResetConnection();
		return false;
	}
	catch (const std::exception &e)
	{
		this->set_error(0, e.what());
		this->set_affected_rows(-1);

		this->client->ResetConnection();
		return false;
	}
}

auto ClickHouseDB::do_insert(const string &table_name, zend_array *values, zend_array *fields) const -> bool
{
	this->set_error(0, "");

	if (!this->is_connected())
		return false;

	if (table_name.empty())
	{
		zend_error(E_WARNING, "Table name is empty");
		return false;
	}

	if (zend_hash_num_elements(values) == 0)
	{
		zend_error(E_WARNING, "Values can't be empty");
		return false;
	}

	vector<zend_string*> fields_data;

	if (!ClickHouseDB::parse_fields(fields, fields_data))
		return false;

	zval *first_row = zend_hash_index_find(values, 0);
	if (first_row == nullptr)
	{
		zend_error(E_WARNING, "Values must be simple array with rows starting from index 0");
		return false;
	}

	zval column_names;
	array_init(&column_names);

	bool value_found = false;
	bool numeric_keys = false;

	string insert_query("INSERT INTO ");
	insert_query.append(table_name);
	insert_query.append(" (");

	zend_ulong column_index;
	zend_string *column_key;
	ZEND_HASH_FOREACH_KEY(Z_ARR_P(first_row), column_index, column_key)
	{
		zend_string *name;

		bool is_numeric_key = (column_key == nullptr);
		if (value_found && is_numeric_key != numeric_keys)
		{
			zend_error(E_WARNING, "Mixing numeric and string field names is not allowed");
			zend_array_destroy(Z_ARR(column_names));
			return false;
		}

		if (is_numeric_key)
		{
			if (column_index >= fields_data.size())
			{
				zend_error(E_WARNING, "Field name is not provided for column %lu at row 0", column_index);
				zend_array_destroy(Z_ARR(column_names));
				return false;
			}

			name = fields_data[column_index];
		}
		else
		{
			name = column_key;

			if (!ClickHouseDB::set_column_index(Z_ARR(column_names), name))
			{
				zend_error(E_WARNING, "Mixing numeric and string field names is not allowed");
				zend_array_destroy(Z_ARR(column_names));
				return false;
			}
		}

		if (value_found)
			insert_query.append(", ");

		insert_query.append(ZSTR_VAL(name), ZSTR_LEN(name));

		value_found = true;
		numeric_keys = is_numeric_key;
	}
	ZEND_HASH_FOREACH_END();

	insert_query.append(") VALUES");

	// ReSharper disable once CppTooWideScopeInitStatement
	size_t columns_count = zend_hash_num_elements(Z_ARR_P(first_row));

	if ((!fields_data.empty() && fields_data.size() != columns_count) || (fields_data.empty() && zend_hash_num_elements(Z_ARR(column_names)) != columns_count))
	{
		zend_error(E_WARNING, "Fields count must be equal to columns count");
		zend_array_destroy(Z_ARR(column_names));
		return false;
	}

	Block description_block = this->client->BeginInsert(Query(insert_query));

	if (description_block.GetColumnCount() != columns_count)
	{
		zend_error(E_WARNING, "Table %s description has %lu columns but %lu requested", table_name.c_str(), description_block.GetColumnCount(), columns_count);
		zend_array_destroy(Z_ARR(column_names));
		this->client->EndInsert();
		return false;
	}

	vector<ColumnRef> columns;
	columns.reserve(columns_count);

	for (size_t i = 0; i < columns_count; i++)
		columns.push_back(create_column(description_block[i]->Type()));

	zend_long rows = 0;
	bool filled = fill_columns(values, columns, Z_ARR(column_names), fields_data, numeric_keys, rows);

	zend_array_destroy(Z_ARR(column_names));

	if (!filled)
	{
		this->client->EndInsert();
		return false;
	}

	Block block;
	for (size_t i = 0; i < columns_count; i++)
	{
		ColumnRef column = columns[i];
		if (description_block[i]->Type()->GetCode() == Type::Code::LowCardinality)
			column = wrap_low_cardinality(column);

		block.AppendColumn(description_block.GetColumnName(i), column);
	}

	block.RefreshRowCount();

	this->client->SendInsertBlock(block);
	this->client->EndInsert();

	this->set_affected_rows(rows);
	return true;
}

auto ClickHouseDB::fill_columns(const zend_array *values, const vector<ColumnRef> &columns, const zend_array *column_names, const vector<zend_string*> &fields_data, bool numeric_keys, zend_long &rows) -> bool
{
	zend_ulong row_index;
	zend_string *row_key;
	zval *row;
	ZEND_HASH_FOREACH_KEY_VAL(values, row_index, row_key, row)
	{
		if (row_key != nullptr)
		{
			zend_error(E_WARNING, "Values key must be number but got string '%s'", ZSTR_VAL(row_key));
			return false;
		}

		if (Z_TYPE_P(row) != IS_ARRAY)
		{
			zend_error(E_WARNING, "Values must be array but got type %d", Z_TYPE_P(row));
			return false;
		}

		if (zend_hash_num_elements(Z_ARR_P(row)) != columns.size())
		{
			zend_error(E_WARNING, "Row %lu has %u columns but %lu expected", row_index, zend_hash_num_elements(Z_ARR_P(row)), columns.size());
			return false;
		}

		rows++;

		zend_ulong column_index;
		zend_string *column_key;
		zval *column_value;
		ZEND_HASH_FOREACH_KEY_VAL(Z_ARR_P(row), column_index, column_key, column_value)
		{
			zend_string *name;
			zend_ulong index;

			bool is_numeric_key = (column_key == nullptr);
			if (is_numeric_key != numeric_keys)
			{
				zend_error(E_WARNING, "Mixing numeric and string field names is not allowed");
				return false;
			}

			if (is_numeric_key)
			{
				if (column_index >= fields_data.size())
				{
					zend_error(E_WARNING, "Field name is not provided for column %lu at row %lu", column_index, row_index);
					return false;
				}

				index = column_index;

				name = fields_data[index];
			}
			else
			{
				name = column_key;

				zval *index_val = zend_hash_find(column_names, name);
				if (index_val == nullptr)
				{
					zend_error(E_WARNING, "Unexpected column '%s', columns must be the same for each row", ZSTR_VAL(name));
					return false;
				}

				index = Z_LVAL_P(index_val);
			}

			if (!append_value(columns[index], column_value, name))
				return false;
		}
		ZEND_HASH_FOREACH_END();
	}
	ZEND_HASH_FOREACH_END();

	return true;
}

auto ClickHouseDB::create_column(const TypeRef &type) -> ColumnRef
{
	if (type->GetCode() == Type::Code::LowCardinality)
		return CreateColumnByType(type->As<LowCardinalityType>()->GetNestedType()->GetName());

	return CreateColumnByType(type->GetName());
}

auto ClickHouseDB::wrap_low_cardinality(const ColumnRef &column) -> ColumnRef
{
	if (auto nullable = column->As<ColumnNullable>())
		return make_shared<ColumnLowCardinality>(nullable);

	return make_shared<ColumnLowCardinality>(column);
}

auto ClickHouseDB::append_value(const ColumnRef &column, zval *value, const zend_string *name) -> bool
{
	auto php_type = Z_TYPE_P(value);

	// ReSharper disable once CppTooWideScope
	Type::Code type = column->Type()->GetCode();

	switch (type)
	{
		case Type::Code::Int8:
			return append_integer<ColumnInt8, int8_t>(column, value, name);
		case Type::Code::Int16:
			return append_integer<ColumnInt16, int16_t>(column, value, name);
		case Type::Code::Int32:
			return append_integer<ColumnInt32, int32_t>(column, value, name);
		case Type::Code::Int64:
			return append_integer<ColumnInt64, int64_t>(column, value, name);
		case Type::Code::Int128:
			return append_integer<ColumnInt128, Int128>(column, value, name);
		case Type::Code::UInt8:
			return append_integer<ColumnUInt8, uint8_t>(column, value, name);
		case Type::Code::UInt16:
			return append_integer<ColumnUInt16, uint16_t>(column, value, name);
		case Type::Code::UInt32:
			return append_integer<ColumnUInt32, uint32_t>(column, value, name);
		case Type::Code::UInt64:
			return append_integer<ColumnUInt64, uint64_t>(column, value, name);
		case Type::Code::UInt128:
			return append_integer<ColumnUInt128, UInt128>(column, value, name);
		case Type::Code::Float32:
		case Type::Code::Float64:
		{
			double number;
			if (php_type == IS_DOUBLE)
				number = Z_DVAL_P(value);
			else if (php_type == IS_LONG)
				number = static_cast<double>(Z_LVAL_P(value));
			else
				return type_mismatch(name, "float");

			if (type == Type::Code::Float32)
				column->As<ColumnFloat32>()->Append(static_cast<float>(number));
			else
				column->As<ColumnFloat64>()->Append(number);
			return true;
		}
		case Type::Code::String:
			if (php_type != IS_STRING)
				return type_mismatch(name, "string");

			column->As<ColumnString>()->Append(string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)));
			return true;
		case Type::Code::FixedString:
		{
			if (php_type != IS_STRING)
				return type_mismatch(name, "string");

			auto fixed = column->As<ColumnFixedString>();
			if (fixed->FixedSize() < Z_STRLEN_P(value))
			{
				zend_error(E_WARNING, "FixedString column '%s' max size %lu < value size %lu", ZSTR_VAL(name), fixed->FixedSize(), Z_STRLEN_P(value));
				return false;
			}

			fixed->Append(string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)));
			return true;
		}
		case Type::Code::Date:
		case Type::Code::Date32:
		{
			time_t timestamp = 0;
			if (php_type == IS_LONG)
				timestamp = Z_LVAL_P(value) * (24 * 60 * 60);
			else if (php_type == IS_STRING)
			{
				std::optional<time_t> parsed = parse_date(Z_STRVAL_P(value));
				if (!parsed)
				{
					zend_error(E_WARNING, "Failed to parse date '%s' for column '%s'", Z_STRVAL_P(value), ZSTR_VAL(name));
					return false;
				}

				timestamp = *parsed;
			}
			else
				return type_mismatch(name, "date string or days number");

			if (type == Type::Code::Date)
				column->As<ColumnDate>()->Append(timestamp);
			else
				column->As<ColumnDate32>()->Append(timestamp);
			return true;
		}
		case Type::Code::DateTime:
		{
			time_t timestamp = 0;
			if (php_type == IS_LONG)
				timestamp = Z_LVAL_P(value);
			else if (php_type == IS_STRING)
			{
				std::optional<time_t> parsed = parse_datetime(Z_STRVAL_P(value));
				if (!parsed)
				{
					zend_error(E_WARNING, "Failed to parse datetime '%s' for column '%s'", Z_STRVAL_P(value), ZSTR_VAL(name));
					return false;
				}

				timestamp = *parsed;
			}
			else
				return type_mismatch(name, "datetime string or timestamp");

			column->As<ColumnDateTime>()->Append(timestamp);
			return true;
		}
		case Type::Code::DateTime64:
		{
			auto datetime = column->As<ColumnDateTime64>();
			int64_t scale = pow10_int64(datetime->GetPrecision());
			int64_t ticks;

			if (php_type == IS_LONG)
				ticks = Z_LVAL_P(value) * scale;
			else if (php_type == IS_DOUBLE)
				ticks = static_cast<int64_t>(std::llround(Z_DVAL_P(value) * static_cast<double>(scale)));
			else if (php_type == IS_STRING)
			{
				std::optional<time_t> seconds = parse_datetime(Z_STRVAL_P(value));
				if (!seconds)
				{
					zend_error(E_WARNING, "Failed to parse datetime '%s' for column '%s'", Z_STRVAL_P(value), ZSTR_VAL(name));
					return false;
				}

				const char *colon = strrchr(Z_STRVAL_P(value), ':');
				ticks = *seconds * scale + parse_fraction(colon != nullptr ? strchr(colon, '.') : nullptr, datetime->GetPrecision());
			}
			else
				return type_mismatch(name, "datetime string, timestamp or float seconds");

			datetime->Append(ticks);
			return true;
		}
		case Type::Code::Time:
		case Type::Code::Time64:
		{
			size_t precision = type == Type::Code::Time64 ? column->As<ColumnTime64>()->GetPrecision() : 0;
			int64_t scale = pow10_int64(precision);
			int64_t ticks;

			if (php_type == IS_LONG)
				ticks = Z_LVAL_P(value) * scale;
			else if (php_type == IS_DOUBLE)
				ticks = static_cast<int64_t>(std::llround(Z_DVAL_P(value) * static_cast<double>(scale)));
			else if (php_type == IS_STRING)
			{
				std::optional<int64_t> parsed = parse_time(Z_STRVAL_P(value), precision);
				if (!parsed)
				{
					zend_error(E_WARNING, "Failed to parse time '%s' for column '%s'", Z_STRVAL_P(value), ZSTR_VAL(name));
					return false;
				}

				ticks = *parsed;
			}
			else
				return type_mismatch(name, "time string or seconds");

			if (type == Type::Code::Time)
				column->As<ColumnTime>()->Append(static_cast<int32_t>(ticks));
			else
				column->As<ColumnTime64>()->Append(ticks);
			return true;
		}
		case Type::Code::Bool:
			if (php_type == IS_TRUE || php_type == IS_FALSE)
				column->As<ColumnBool>()->Append(php_type == IS_TRUE);
			else if (php_type == IS_LONG)
				column->As<ColumnBool>()->Append(Z_LVAL_P(value) != 0);
			else
				return type_mismatch(name, "bool");

			return true;
		case Type::Code::Nullable:
		{
			auto nullable = column->As<ColumnNullable>();

			nullable->Append(php_type == IS_NULL);

			if (php_type == IS_NULL)
				return append_default(nullable->Nested(), name);

			return append_value(nullable->Nested(), value, name);
		}
		case Type::Code::LowCardinality:
		{
			if (php_type != IS_STRING)
				return type_mismatch(name, "string");

			if (auto strings = column->As<ColumnLowCardinalityT<ColumnString>>())
			{
				strings->Append(string(Z_STRVAL_P(value), Z_STRLEN_P(value)));
				return true;
			}

			if (auto fixed_strings = column->As<ColumnLowCardinalityT<ColumnFixedString>>())
			{
				fixed_strings->Append(string(Z_STRVAL_P(value), Z_STRLEN_P(value)));
				return true;
			}

			zend_error(E_WARNING, "Nested %s is unsupported for column '%s'", column->Type()->GetName().c_str(), ZSTR_VAL(name));
			return false;
		}
		case Type::Code::Array:
		{
			if (php_type != IS_ARRAY)
				return type_mismatch(name, "array");

			ColumnRef elements = CreateColumnByType(column->Type()->As<ArrayType>()->GetItemType()->GetName());

			zval *element;
			ZEND_HASH_FOREACH_VAL(Z_ARR_P(value), element)
			{
				if (!append_value(elements, element, name))
					return false;
			}
			ZEND_HASH_FOREACH_END();

			column->As<ColumnArray>()->AppendAsColumn(elements);
			return true;
		}
		case Type::Code::Tuple:
		{
			if (php_type != IS_ARRAY)
				return type_mismatch(name, "array");

			auto tuple = column->As<ColumnTuple>();
			if (zend_hash_num_elements(Z_ARR_P(value)) != tuple->TupleSize())
			{
				zend_error(E_WARNING, "Tuple column '%s' has %lu elements but %u given", ZSTR_VAL(name), tuple->TupleSize(), zend_hash_num_elements(Z_ARR_P(value)));
				return false;
			}

			size_t i = 0;
			zval *element;
			ZEND_HASH_FOREACH_VAL(Z_ARR_P(value), element)
			{
				if (!append_value(tuple->At(i++), element, name))
					return false;
			}
			ZEND_HASH_FOREACH_END();

			return true;
		}
		case Type::Code::Map:
			if (php_type != IS_ARRAY)
				return type_mismatch(name, "array");

			return append_map(column, Z_ARR_P(value), name);
		case Type::Code::Point:
		{
			std::tuple<double, double> point;
			if (!php_to_geo(value, point, name))
				return false;

			column->As<ColumnPoint>()->Append(point);
			return true;
		}
		case Type::Code::Ring:
		{
			vector<std::tuple<double, double>> ring;
			if (!php_to_geo(value, ring, name))
				return false;

			column->As<ColumnRing>()->Append(ring);
			return true;
		}
		case Type::Code::Polygon:
		{
			vector<vector<std::tuple<double, double>>> polygon;
			if (!php_to_geo(value, polygon, name))
				return false;

			column->As<ColumnPolygon>()->Append(polygon);
			return true;
		}
		case Type::Code::MultiPolygon:
		{
			vector<vector<vector<std::tuple<double, double>>>> multi_polygon;
			if (!php_to_geo(value, multi_polygon, name))
				return false;

			column->As<ColumnMultiPolygon>()->Append(multi_polygon);
			return true;
		}
		case Type::Code::Enum8:
		case Type::Code::Enum16:
		{
			if (php_type == IS_STRING)
			{
				string enum_name(Z_STRVAL_P(value), Z_STRLEN_P(value));

				if (type == Type::Code::Enum8)
					column->As<ColumnEnum8>()->Append(enum_name);
				else
					column->As<ColumnEnum16>()->Append(enum_name);
				return true;
			}

			if (php_type == IS_LONG)
			{
				if (type == Type::Code::Enum8)
					column->As<ColumnEnum8>()->Append(static_cast<int8_t>(Z_LVAL_P(value)), true);
				else
					column->As<ColumnEnum16>()->Append(static_cast<int16_t>(Z_LVAL_P(value)), true);
				return true;
			}

			return type_mismatch(name, "enum name or value");
		}
		case Type::Code::UUID:
		{
			if (php_type != IS_STRING)
				return type_mismatch(name, "uuid string");

			std::optional<UUID> uuid = string_to_uuid(string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)));
			if (!uuid)
			{
				zend_error(E_WARNING, "Failed to parse UUID '%s' for column '%s'", Z_STRVAL_P(value), ZSTR_VAL(name));
				return false;
			}

			column->As<ColumnUUID>()->Append(*uuid);
			return true;
		}
		case Type::Code::IPv4:
			if (php_type == IS_STRING)
			{
				column->As<ColumnIPv4>()->Append(string(Z_STRVAL_P(value), Z_STRLEN_P(value)));
				return true;
			}

			if (php_type == IS_LONG)
			{
				column->As<ColumnIPv4>()->Append(htonl(static_cast<uint32_t>(Z_LVAL_P(value))));
				return true;
			}

			return type_mismatch(name, "ip string or number");
		case Type::Code::IPv6:
			if (php_type != IS_STRING)
				return type_mismatch(name, "ip string");

			column->As<ColumnIPv6>()->Append(string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)));
			return true;
		case Type::Code::Decimal:
		case Type::Code::Decimal32:
		case Type::Code::Decimal64:
		case Type::Code::Decimal128:
		{
			auto decimal = column->As<ColumnDecimal>();

			if (php_type == IS_STRING)
				decimal->Append(string(Z_STRVAL_P(value), Z_STRLEN_P(value)));
			else if (php_type == IS_LONG)
				decimal->Append(std::to_string(Z_LVAL_P(value)));
			else if (php_type == IS_DOUBLE)
			{
				char buffer[64];
				snprintf(buffer, sizeof(buffer), "%.*f", static_cast<int>(decimal->GetScale()), Z_DVAL_P(value));
				decimal->Append(string(buffer));
			}
			else
				return type_mismatch(name, "decimal string or number");

			return true;
		}
		default:
			zend_error(E_WARNING, "Type %s (%d) is unsupported for column '%s'", column->Type()->GetName().c_str(), type, ZSTR_VAL(name));
			return false;
	}
}

auto ClickHouseDB::append_default(const ColumnRef &column, const zend_string *name) -> bool
{
	// ReSharper disable once CppTooWideScope
	Type::Code type = column->Type()->GetCode();

	switch (type)
	{
		case Type::Code::Int8:
			column->As<ColumnInt8>()->Append(0);
			return true;
		case Type::Code::Int16:
			column->As<ColumnInt16>()->Append(0);
			return true;
		case Type::Code::Int32:
			column->As<ColumnInt32>()->Append(0);
			return true;
		case Type::Code::Int64:
			column->As<ColumnInt64>()->Append(0);
			return true;
		case Type::Code::Int128:
			column->As<ColumnInt128>()->Append(Int128(0));
			return true;
		case Type::Code::UInt8:
			column->As<ColumnUInt8>()->Append(0);
			return true;
		case Type::Code::UInt16:
			column->As<ColumnUInt16>()->Append(0);
			return true;
		case Type::Code::UInt32:
			column->As<ColumnUInt32>()->Append(0);
			return true;
		case Type::Code::UInt64:
			column->As<ColumnUInt64>()->Append(0);
			return true;
		case Type::Code::UInt128:
			column->As<ColumnUInt128>()->Append(UInt128(0));
			return true;
		case Type::Code::Float32:
			column->As<ColumnFloat32>()->Append(0.f);
			return true;
		case Type::Code::Float64:
			column->As<ColumnFloat64>()->Append(0.);
			return true;
		case Type::Code::String:
			column->As<ColumnString>()->Append(string_view());
			return true;
		case Type::Code::FixedString:
			column->As<ColumnFixedString>()->Append(string_view());
			return true;
		case Type::Code::Date:
			column->As<ColumnDate>()->Append(0);
			return true;
		case Type::Code::Date32:
			column->As<ColumnDate32>()->Append(0);
			return true;
		case Type::Code::DateTime:
			column->As<ColumnDateTime>()->Append(0);
			return true;
		case Type::Code::DateTime64:
			column->As<ColumnDateTime64>()->Append(0);
			return true;
		case Type::Code::Time:
			column->As<ColumnTime>()->Append(0);
			return true;
		case Type::Code::Time64:
			column->As<ColumnTime64>()->Append(0);
			return true;
		case Type::Code::Bool:
			column->As<ColumnBool>()->Append(false);
			return true;
		case Type::Code::Nullable:
		{
			auto nullable = column->As<ColumnNullable>();

			nullable->Append(true);
			return append_default(nullable->Nested(), name);
		}
		case Type::Code::LowCardinality:
			if (auto strings = column->As<ColumnLowCardinalityT<ColumnString>>())
			{
				strings->Append(string());
				return true;
			}

			if (auto fixed_strings = column->As<ColumnLowCardinalityT<ColumnFixedString>>())
			{
				fixed_strings->Append(string());
				return true;
			}

			zend_error(E_WARNING, "Nested %s is unsupported for column '%s'", column->Type()->GetName().c_str(), ZSTR_VAL(name));
			return false;
		case Type::Code::Array:
			column->As<ColumnArray>()->AppendAsColumn(CreateColumnByType(column->Type()->As<ArrayType>()->GetItemType()->GetName()));
			return true;
		case Type::Code::Tuple:
		{
			auto tuple = column->As<ColumnTuple>();

			for (size_t i = 0; i < tuple->TupleSize(); i++)
			{
				if (!append_default(tuple->At(i), name))
					return false;
			}

			return true;
		}
		case Type::Code::Point:
			column->As<ColumnPoint>()->Append(std::make_tuple(0., 0.));
			return true;
		case Type::Code::Ring:
			column->As<ColumnRing>()->Append(vector<std::tuple<double, double>>());
			return true;
		case Type::Code::Polygon:
			column->As<ColumnPolygon>()->Append(vector<vector<std::tuple<double, double>>>());
			return true;
		case Type::Code::MultiPolygon:
			column->As<ColumnMultiPolygon>()->Append(vector<vector<vector<std::tuple<double, double>>>>());
			return true;
		case Type::Code::Map:
		{
			zval empty;
			array_init(&empty);

			bool result = append_map(column, Z_ARR(empty), name);

			zval_ptr_dtor(&empty);
			return result;
		}
		case Type::Code::Enum8:
			column->As<ColumnEnum8>()->Append(static_cast<int8_t>(column->Type()->As<EnumType>()->BeginValueToName()->first));
			return true;
		case Type::Code::Enum16:
			column->As<ColumnEnum16>()->Append(column->Type()->As<EnumType>()->BeginValueToName()->first);
			return true;
		case Type::Code::UUID:
			column->As<ColumnUUID>()->Append(UUID{0, 0});
			return true;
		case Type::Code::IPv4:
			column->As<ColumnIPv4>()->Append(static_cast<uint32_t>(0));
			return true;
		case Type::Code::IPv6:
			column->As<ColumnIPv6>()->Append(in6addr_any);
			return true;
		case Type::Code::Decimal:
		case Type::Code::Decimal32:
		case Type::Code::Decimal64:
		case Type::Code::Decimal128:
			column->As<ColumnDecimal>()->Append(Int128(0));
			return true;
		default:
			zend_error(E_WARNING, "Type %s (%d) is unsupported for column '%s'", column->Type()->GetName().c_str(), type, ZSTR_VAL(name));
			return false;
	}
}

auto ClickHouseDB::append_map(const ColumnRef &column, const zend_array *pairs, const zend_string *name) -> bool
{
	auto map_type = column->Type()->As<MapType>();

	ColumnRef keys = CreateColumnByType(map_type->GetKeyType()->GetName());
	ColumnRef items = CreateColumnByType(map_type->GetValueType()->GetName());

	zend_string *key_string;
	zend_ulong key_index;
	zval *item;
	ZEND_HASH_FOREACH_KEY_VAL(pairs, key_index, key_string, item)
	{
		zval key;
		if (key_string != nullptr)
			ZVAL_STR(&key, key_string);
		else
			ZVAL_LONG(&key, static_cast<zend_long>(key_index));

		if (!append_value(keys, &key, name) || !append_value(items, item, name))
			return false;
	}
	ZEND_HASH_FOREACH_END();

	auto row = make_shared<ColumnArray>(make_shared<ColumnTuple>(vector{keys->CloneEmpty(), items->CloneEmpty()}));
	row->AppendAsColumn(make_shared<ColumnTuple>(vector{keys, items}));

	column->As<ColumnMap>()->Append(make_shared<ColumnMap>(row));
	return true;
}

auto ClickHouseDB::parse_date(const char *text) -> std::optional<time_t>
{
	tm tm_time{};

	if (strptime(text, DATE_FORMAT, &tm_time) == nullptr)
		return std::nullopt;

	return timegm(&tm_time);
}

auto ClickHouseDB::parse_datetime(const char *text) -> std::optional<time_t>
{
	zval date;
	php_date_instantiate(php_date_get_date_ce(), &date);

	php_date_obj *date_object = Z_PHPDATE_P(&date);
	std::optional<time_t> timestamp;

	if (php_date_initialize(date_object, text, strlen(text), nullptr, nullptr, 0))
		timestamp = static_cast<time_t>(date_object->time->sse);

	zval_ptr_dtor(&date);
	return timestamp;
}

auto ClickHouseDB::parse_fraction(const char *text, size_t precision) -> int64_t
{
	if (text == nullptr || *text != '.')
		return 0;

	int64_t fraction = 0;
	size_t digits = 0;

	for (const char *c = text + 1; *c >= '0' && *c <= '9'; c++)
	{
		if (digits < precision)
			fraction = fraction * 10 + (*c - '0');
		digits++;
	}

	for (size_t i = digits; i < precision; i++)
		fraction *= 10;

	return fraction;
}

auto ClickHouseDB::parse_time(const char *text, size_t precision) -> std::optional<int64_t>
{
	bool negative = *text == '-';
	if (negative)
		text++;

	int hours = 0;
	int minutes = 0;
	int seconds = 0;
	int consumed = 0;

	if (sscanf(text, "%d:%d:%d%n", &hours, &minutes, &seconds, &consumed) != 3)
		return std::nullopt;

	int64_t ticks = (hours * 3600LL + minutes * 60 + seconds) * pow10_int64(precision) + parse_fraction(text + consumed, precision);

	return negative ? -ticks : ticks;
}

auto ClickHouseDB::type_mismatch(const zend_string *name, const char *expected) -> bool
{
	zend_error(E_WARNING, "Value type and declared type mismatch for column '%s', %s expected", ZSTR_VAL(name), expected);
	return false;
}

auto ClickHouseDB::is_connected() const -> bool
{
	if (this->client)
		return true;

	zend_error(E_WARNING, "ClickHouse not connected");
	return false;
}

void ClickHouseDB::set_error(zend_long code, const char *message) const
{
#if PHP_API_VERSION >= 20200930
	zend_update_property_long(this->zend_this->ce, this->zend_this, "errno", sizeof("errno") - 1, code);
	zend_update_property_string(this->zend_this->ce, this->zend_this, "error", sizeof("error") - 1, message);
#else
	zval zv;
	ZVAL_OBJ(&zv, this->zend_this);

	zend_update_property_long(this->zend_this->ce, &zv, "errno", sizeof("errno") - 1, code);
	zend_update_property_string(this->zend_this->ce, &zv, "error", sizeof("error") - 1, message);
#endif
}

void ClickHouseDB::set_affected_rows(zend_long value) const
{
#if PHP_API_VERSION >= 20200930
	zend_update_property_long(this->zend_this->ce, this->zend_this, "affected_rows", sizeof("affected_rows") - 1, value);
#else
	zval zv;
	ZVAL_OBJ(&zv, this->zend_this);

	zend_update_property_long(this->zend_this->ce, &zv, "affected_rows", sizeof("affected_rows") - 1, value);
#endif
}

auto ClickHouseDB::parse_fields(const zend_array *fields, vector<zend_string *> &data) -> bool
{
	if (fields == nullptr)
		return true;

	unordered_set<string> uniques;

	zend_ulong index;
	zend_string *key;
	zval *field;
	ZEND_HASH_FOREACH_KEY_VAL(fields, index, key, field)
	{
		if (key != nullptr)
		{
			zend_error(E_WARNING, "Field key must be number but got string '%s'", ZSTR_VAL(key));
			return false;
		}

		if (Z_TYPE_P(field) != IS_STRING)
		{
			zend_error(E_WARNING, "Field must be string but got type %d", Z_TYPE_P(field));
			return false;
		}

		if (index != data.size())
		{
			zend_error(E_WARNING, "Field keys must go continuously in ascending order, key %lu received but %lu expected", index, data.size());
			return false;
		}

		// ReSharper disable once CppTooWideScopeInitStatement
		auto [iter, success] = uniques.insert(string(Z_STRVAL_P(field), Z_STRLEN_P(field)));
		if (!success)
		{
			zend_error(E_WARNING, "Field name '%s' listed twice", Z_STRVAL_P(field));
			return false;
		}

		data.push_back(Z_STR_P(field));
	}
	ZEND_HASH_FOREACH_END();

	if (data.empty())
	{
		zend_error(E_WARNING, "Fields can't be empty");
		return false;
	}

	return true;
}

auto ClickHouseDB::set_column_index(zend_array *names, zend_string *name) -> bool
{
	// ReSharper disable once CppTooWideScopeInitStatement
	zval *index_val = zend_hash_find(names, name);
	if (index_val != nullptr)
		return false;

	zend_ulong index = zend_hash_num_elements(names);

	zval tmp;
	ZVAL_LONG(&tmp, index);

	zend_hash_add(names, name, &tmp);
	return true;
}