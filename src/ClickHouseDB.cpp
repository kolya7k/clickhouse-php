#include "ClickHouseDB.h"

#include "ClickHouseResult.h"

__inline static auto clickhouse_result_new(deque<Block> blocks, size_t rows_count, long int timezone_offset) -> zend_object *
{
	auto obj = static_cast<ClickHouseResultObject*>(zend_object_alloc(sizeof(ClickHouseResultObject), clickhouse_result_class_entry));

	zend_object_std_init(&obj->std, clickhouse_result_class_entry);
	object_properties_init(&obj->std, clickhouse_result_class_entry);

	obj->std.handlers = &clickhouse_object_result_handlers;

	obj->impl = new ClickHouseResult(&obj->std, std::move(blocks), rows_count, timezone_offset);

	return &obj->std;
}

ClickHouseDB::ClickHouseDB(zend_object *zend_this):
	zend_this(zend_this)
{
	time_t value = 0;
	tm tm_time{};
	localtime_r(&value, &tm_time);

	this->timezone_offset = tm_time.tm_gmtoff;
}

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
		options.SetPort(port);
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
	catch (ServerException &e)
	{
		success = false;

		this->set_error(e.GetCode(), e.what());
		this->set_affected_rows(-1);

		this->client->ResetConnection();
		return nullptr;
	}
	catch (std::runtime_error &e)
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

	return clickhouse_result_new(std::move(blocks), rows_count, this->timezone_offset);
}

auto ClickHouseDB::insert(const string &table_name, zend_array *values, zend_array *fields) const -> bool
{
	try
	{
		return this->do_insert(table_name, values, fields);
	}
	catch (ServerException &e)
	{
		this->set_error(e.GetCode(), e.what());
		this->set_affected_rows(-1);

		this->client->ResetConnection();
		return false;
	}
	catch (std::exception &e)
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

	Bucket *first_row_column_bucket;
	ZEND_HASH_FOREACH_BUCKET(Z_ARR_P(first_row), first_row_column_bucket)
	{
		zend_string *name;

		bool is_numeric_key = (first_row_column_bucket->key == nullptr);
		if (value_found && is_numeric_key != numeric_keys)
		{
			zend_error(E_WARNING, "Mixing numeric and string field names is not allowed");
			zend_array_destroy(Z_ARR(column_names));
			return false;
		}

		if (is_numeric_key)
		{
			if (first_row_column_bucket->h >= fields_data.size())
			{
				zend_error(E_WARNING, "Field name is not provided for column %lu at row 0", first_row_column_bucket->h);
				zend_array_destroy(Z_ARR(column_names));
				return false;
			}

			name = fields_data[first_row_column_bucket->h];
		}
		else
		{
			name = first_row_column_bucket->key;

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
		return false;
	}

	Block description_block;

	this->client->InsertQuery(insert_query, [&description_block] (const Block &block)
	{
		description_block = block;
	});

	Block block;
	zend_long rows = 0;

	Bucket *row_bucket;
	ZEND_HASH_FOREACH_BUCKET(values, row_bucket)
	{
		if (row_bucket->key != nullptr)
		{
			zend_error(E_WARNING, "Values key must be number but got string '%s'", ZSTR_VAL(row_bucket->key));
			zend_array_destroy(Z_ARR(column_names));
			return false;
		}

		if (Z_TYPE(row_bucket->val) != IS_ARRAY)
		{
			zend_error(E_WARNING, "Values must be array but got type %d", Z_TYPE(row_bucket->val));
			zend_array_destroy(Z_ARR(column_names));
			return false;
		}

		rows++;

		Bucket *column_bucket;
		ZEND_HASH_FOREACH_BUCKET(Z_ARR(row_bucket->val), column_bucket)
		{
			zend_string *name;
			zend_ulong index;

			bool is_numeric_key = (column_bucket->key == nullptr);
			if (is_numeric_key != numeric_keys)
			{
				zend_error(E_WARNING, "Mixing numeric and string field names is not allowed");
				zend_array_destroy(Z_ARR(column_names));
				return false;
			}

			if (is_numeric_key)
			{
				if (column_bucket->h >= fields_data.size())
				{
					zend_error(E_WARNING, "Field name is not provided for column %lu at row %lu", column_bucket->h, row_bucket->h);
					zend_array_destroy(Z_ARR(column_names));
					return false;
				}

				index = column_bucket->h;

				name = fields_data[index];
			}
			else
			{
				name = column_bucket->key;

				zval *index_val = zend_hash_find(Z_ARR(column_names), name);
				if (index_val == nullptr)
				{
					zend_error(E_WARNING, "Unexpected column '%s', columns must be the same for each row", ZSTR_VAL(name));
					zend_array_destroy(Z_ARR(column_names));
					return false;
				}

				index = Z_LVAL_P(index_val);
			}

			if (!ClickHouseDB::add_by_type(block, name, index, &column_bucket->val, description_block[index]))
			{
				zend_array_destroy(Z_ARR(column_names));
				return false;
			}
		}
		ZEND_HASH_FOREACH_END();
	}
	ZEND_HASH_FOREACH_END();

	zend_array_destroy(Z_ARR(column_names));

	block.RefreshRowCount();

	this->client->InsertData(block);

	this->set_affected_rows(rows);
	return true;
}

auto ClickHouseDB::add_by_type(Block &block, zend_string *name, zend_ulong index, zval *z_value, const ColumnRef &description_column, bool nullable) -> bool
{
	auto php_type = Z_TYPE_P(z_value);
	bool types_match;

	Type::Code type = description_column->Type()->GetCode();
	switch (type)
	{
//		case Type::Code::Void:
		case Type::Code::Int8:
		case Type::Code::Int16:
		case Type::Code::Int32:
		case Type::Code::Int64:
		case Type::Code::UInt8:
		case Type::Code::UInt16:
		case Type::Code::UInt32:
		case Type::Code::UInt64:
			types_match = (php_type == IS_LONG || (nullable && php_type == IS_NULL));
			break;
		case Type::Code::Float32:
		case Type::Code::Float64:
			types_match = (php_type == IS_DOUBLE || (nullable && php_type == IS_NULL));
			break;
		case Type::Code::String:
		case Type::Code::FixedString:
			types_match = (php_type == IS_STRING || (nullable && php_type == IS_NULL));
			break;
		case Type::Code::DateTime:
		case Type::Code::Date:
			types_match = (php_type == IS_STRING || php_type == IS_LONG || (nullable && php_type == IS_NULL));
			break;
//		case Type::Code::Array:
		case Type::Code::Nullable:
			// Checked in nested call
			types_match = !nullable;
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
			zend_error(E_WARNING, "Value type %d is unsupported", type);
			return false;
	}

	if (!types_match)
	{
		zend_error(E_WARNING, "Value type and declared type mismatch for value '%s'", ZSTR_VAL(name));
		return false;
	}

	switch (type)
	{
//		case Type::Code::Void:
		case Type::Code::Int8:
			add_value<ColumnInt8>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Int16:
			add_value<ColumnInt16>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Int32:
			add_value<ColumnInt32>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Int64:
			add_value<ColumnInt64>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt8:
			add_value<ColumnUInt8>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt16:
			add_value<ColumnUInt16>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt32:
			add_value<ColumnUInt32>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt64:
			add_value<ColumnUInt64>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Float32:
			add_value<ColumnFloat32>(block, name, index, (php_type != IS_NULL) ? Z_DVAL_P(z_value) : 0., nullable, php_type == IS_NULL);
			break;
		case Type::Code::Float64:
			add_value<ColumnFloat64>(block, name, index, (php_type != IS_NULL) ? Z_DVAL_P(z_value) : 0., nullable, php_type == IS_NULL);
			break;
		case Type::Code::String:
			add_value<ColumnString>(block, name, index, (php_type != IS_NULL) ? string_view(Z_STRVAL_P(z_value), Z_STRLEN_P(z_value)) : "", nullable, php_type == IS_NULL);
			break;
		case Type::Code::FixedString:
		{
			auto column = description_column->As<ColumnFixedString>();

			if (php_type != IS_NULL && column->FixedSize() < Z_STRLEN_P(z_value))
			{
				zend_error(E_WARNING, "FixedString column max size %lu < value size %lu", column->FixedSize(), Z_STRLEN_P(z_value));
				return false;
			}

			add_fixed_string(block, name, index, php_type != IS_NULL ? string_view(Z_STRVAL_P(z_value), Z_STRLEN_P(z_value)) : "", column->FixedSize(), nullable, php_type == IS_NULL);
			break;
		}
		case Type::Code::DateTime:
		//case Type::Code::DateTime64:
		{
			time_t timestamp = 0;
			if (php_type == IS_LONG)
				timestamp = Z_LVAL_P(z_value);
			else if (php_type == IS_STRING)
			{
				tm tm_time{};
				if (strptime(Z_STRVAL_P(z_value), DATETIME_FORMAT, &tm_time) == nullptr)
				{
					zend_error(E_WARNING, "Failed to parse date '%s' from format '%s'", Z_STRVAL_P(z_value), DATETIME_FORMAT);
					return false;
				}

				timestamp = mktime(&tm_time);
			}

			add_value<ColumnDateTime>(block, name, index, timestamp, nullable, php_type == IS_NULL);
			break;
		}
		case Type::Code::Date:
		//case Type::Code::Date32:
		{
			time_t timestamp = 0;
			if (php_type == IS_LONG)
				timestamp = Z_LVAL_P(z_value) * (24 * 60 * 60);
			else if (php_type == IS_STRING)
			{
				tm tm_time{};
				if (strptime(Z_STRVAL_P(z_value), DATE_FORMAT, &tm_time) == nullptr)
				{
					zend_error(E_WARNING, "Failed to parse date '%s' from format '%s'", Z_STRVAL_P(z_value), DATE_FORMAT);
					return false;
				}

				timestamp = timegm(&tm_time);
			}

			add_value<ColumnDate>(block, name, index, timestamp, nullable, php_type == IS_NULL);
			break;
		}
//		case Type::Code::Array:
		case Type::Code::Nullable:
			return add_by_type(block, name, index, z_value, description_column->As<ColumnNullable>()->Nested(), true);
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
			zend_error(E_WARNING, "Value type %d is unsupported", type);
			return false;
	}

	return true;
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

auto ClickHouseDB::parse_fields(zend_array *fields, vector<zend_string *> &data) -> bool
{
	if (fields == nullptr)
		return true;

	unordered_set<string> uniques;

	Bucket *bucket;
	ZEND_HASH_FOREACH_BUCKET(fields, bucket)
	{
		if (bucket->key != nullptr)
		{
			zend_error(E_WARNING, "Field key must be number but got string '%s'", ZSTR_VAL(bucket->key));
			return false;
		}

		if (Z_TYPE(bucket->val) != IS_STRING)
		{
			zend_error(E_WARNING, "Field must be string but got type %d", Z_TYPE(bucket->val));
			return false;
		}

		if (bucket->h != data.size())
		{
			zend_error(E_WARNING, "Field keys must go continuously in ascending order, key %lu received but %lu expected", bucket->h, data.size());
			return false;
		}

		// ReSharper disable once CppTooWideScopeInitStatement
		auto [iter, success] = uniques.insert(string(Z_STRVAL(bucket->val), Z_STRLEN(bucket->val)));
		if (!success)
		{
			zend_error(E_WARNING, "Field name '%s' listed twice", Z_STRVAL(bucket->val));
			return false;
		}

		data.push_back(Z_STR(bucket->val));
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

void ClickHouseDB::add_fixed_string(Block &block, const zend_string *name, zend_ulong index, const string_view &value, zend_long size, bool nullable, bool is_null)
{
	if (block.GetColumnCount() <= index)
	{
		if (nullable)
		{
			auto nested = make_shared<ColumnFixedString>(size);
			auto nulls = make_shared<ColumnUInt8>();

			block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnNullable>(nested, nulls));
		}
		else
			block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnFixedString>(size));
	}

	if (nullable)
	{
		auto column = block[index]->As<ColumnNullable>();

		column->Nested()->As<ColumnFixedString>()->Append(value);
		column->Nulls()->As<ColumnUInt8>()->Append(is_null ? 1 : 0);
	}
	else
		block[index]->As<ColumnFixedString>()->Append(value);
}