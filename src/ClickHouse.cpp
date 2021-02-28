#include "ClickHouse.h"

const unordered_map<string, Type::Code> ClickHouse::types_names
{
//	{"Void", Type::Code::Void},
	{"Int8", Type::Code::Int8},
	{"Int16", Type::Code::Int16},
	{"Int32", Type::Code::Int32},
	{"Int64", Type::Code::Int64},
	{"UInt8", Type::Code::UInt8},
	{"UInt16", Type::Code::UInt16},
	{"UInt32", Type::Code::UInt32},
	{"UInt64", Type::Code::UInt64},
	{"Float32", Type::Code::Float32},
	{"Float64", Type::Code::Float64},
	{"String", Type::Code::String},
	{"FixedString", Type::Code::FixedString},
	{"DateTime", Type::Code::DateTime},
	{"Date", Type::Code::Date},
//	{"Array", Type::Code::Array},
	{"Nullable", Type::Code::Nullable},
//	{"Tuple", Type::Code::Tuple},
//	{"Enum8", Type::Code::Enum8},
//	{"Enum16", Type::Code::Enum16},
//	{"UUID", Type::Code::UUID},
//	{"IPv4", Type::Code::IPv4},
//	{"IPv6", Type::Code::IPv6},
//	{"Int128", Type::Code::Int128},
//	{"Decimal", Type::Code::Decimal},
//	{"Decimal32", Type::Code::Decimal32},
//	{"Decimal64", Type::Code::Decimal64},
//	{"Decimal128", Type::Code::Decimal128},
//	{"LowCardinality", Type::Code::LowCardinality}
};

__inline static zend_object* clickhouse_result_new(deque<Block> blocks, size_t rows_count)
{
	ClickHouseResultObject *obj = static_cast<ClickHouseResultObject*>(zend_object_alloc(sizeof(ClickHouseResultObject), clickhouse_result_class_entry));

	zend_object_std_init(&obj->std, clickhouse_result_class_entry);
	object_properties_init(&obj->std, clickhouse_result_class_entry);

	obj->std.handlers = &clickhouse_object_result_handlers;

	obj->impl = new ClickHouseResult(&obj->std, move(blocks), rows_count);

	return &obj->std;
}

ClickHouse::ClickHouse(zend_object *zend_this):
	zend_this(zend_this)
{}

void ClickHouse::connect(zend_string *host, zend_string *username, zend_string *passwd, zend_string *dbname, zend_long port)
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

zend_object* ClickHouse::query(const string &query, bool &success)
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

			rows_count += block.GetRowCount();
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
	catch (std::system_error &e)
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

	return clickhouse_result_new(move(blocks), rows_count);
}

bool ClickHouse::insert(const string &table_name, zend_array *values, zend_array *types, zend_array *fields)
{
	this->set_error(0, "");

	if (!this->is_connected())
		return false;

	if (table_name.empty())
	{
		zend_error(E_WARNING, "Table name is empty");
		return false;
	}

	vector<pair<Type::Code, string>> types_data;
	vector<zend_string*> fields_data;

	if (fields != nullptr)
	{
		if (!ClickHouse::parse_types(types, types_data))
			return false;

		if (!ClickHouse::parse_fields(fields, fields_data))
			return false;

		if (types_data.size() != fields_data.size())
		{
			zend_error(E_WARNING, "Types and fields arrays have different sizes %lu != %lu", types_data.size(), fields_data.size());
			return false;
		}
	}

	zval column_names;
	array_init(&column_names);

	Block block;

	bool value_found = false;
	bool numeric_keys = false;
	zend_long rows = 0;

	Bucket *values_bucket;
	ZEND_HASH_FOREACH_BUCKET(values, values_bucket)
	{
		if (values_bucket->key != nullptr)
		{
			zend_error(E_WARNING, "Values key must be number but got string '%s'", ZSTR_VAL(values_bucket->key));
			zend_array_destroy(Z_ARR(column_names));
			return false;
		}

		if (Z_TYPE(values_bucket->val) != IS_ARRAY)
		{
			zend_error(E_WARNING, "Values must be array but got type %d", Z_TYPE(values_bucket->val));
			zend_array_destroy(Z_ARR(column_names));
			return false;
		}

		rows++;

		Bucket *value_bucket;
		ZEND_HASH_FOREACH_BUCKET(Z_ARR(values_bucket->val), value_bucket)
		{
			zend_string *name;
			zend_ulong index;

			Type::Code type;
			string type_param;

			if (value_bucket->key == nullptr)
			{
				if (value_bucket->h >= fields_data.size())
				{
					zend_error(E_WARNING, "Field name is not provided for row %lu and value %lu", values_bucket->h, value_bucket->h);
					zend_array_destroy(Z_ARR(column_names));
					return false;
				}

				if (value_found && !numeric_keys)
				{
					zend_error(E_WARNING, "Mixing numeric and string field names is not allowed");
					zend_array_destroy(Z_ARR(column_names));
					return false;
				}

				value_found = true;
				numeric_keys = true;

				index = value_bucket->h;

				name = fields_data[index];
				type = types_data[index].first;
				type_param = types_data[index].second;
			}
			else
			{
				if (value_found && numeric_keys)
				{
					zend_error(E_WARNING, "Mixing numeric and string field names is not allowed");
					zend_array_destroy(Z_ARR(column_names));
					return false;
				}

				value_found = true;
				numeric_keys = false;

				name = value_bucket->key;

				index = ClickHouse::get_column_index(Z_ARR(column_names), name);
				type = ClickHouse::get_type(types, name, type_param);
			}

			try
			{
				if (!ClickHouse::add_by_type(block, name, index, &value_bucket->val, type, type_param))
				{
					zend_array_destroy(Z_ARR(column_names));
					return false;
				}
			}
			catch (std::runtime_error &e)
			{
				this->set_error(0, e.what());
				this->set_affected_rows(-1);

				this->client->ResetConnection();
				return false;
			}
		}
		ZEND_HASH_FOREACH_END();
	}
	ZEND_HASH_FOREACH_END();

	zend_array_destroy(Z_ARR(column_names));

	try
	{
		block.RefreshRowCount();

		this->client->Insert(table_name, block);
	}
	catch (ServerException &e)
	{
		this->set_error(e.GetCode(), e.what());
		this->set_affected_rows(-1);

		this->client->ResetConnection();
		return false;
	}

	this->set_affected_rows(rows);
	return true;
}

bool ClickHouse::add_by_type(Block &block, zend_string *name, zend_ulong index, zval *z_value, Type::Code type, const string &type_param, bool nullable)
{
	if (type == static_cast<Type::Code>(-1))
		return false;

	auto php_type = Z_TYPE_P(z_value);
	bool types_match;

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
			ClickHouse::add_value<ColumnInt8>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Int16:
			ClickHouse::add_value<ColumnInt16>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Int32:
			ClickHouse::add_value<ColumnInt32>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Int64:
			ClickHouse::add_value<ColumnInt64>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt8:
			ClickHouse::add_value<ColumnUInt8>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt16:
			ClickHouse::add_value<ColumnUInt16>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt32:
			ClickHouse::add_value<ColumnUInt32>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::UInt64:
			ClickHouse::add_value<ColumnUInt64>(block, name, index, (php_type != IS_NULL) ? Z_LVAL_P(z_value) : 0, nullable, php_type == IS_NULL);
			break;
		case Type::Code::Float32:
			ClickHouse::add_value<ColumnFloat32>(block, name, index, (php_type != IS_NULL) ? Z_DVAL_P(z_value) : 0., nullable, php_type == IS_NULL);
			break;
		case Type::Code::Float64:
			ClickHouse::add_value<ColumnFloat64>(block, name, index, (php_type != IS_NULL) ? Z_DVAL_P(z_value) : 0., nullable, php_type == IS_NULL);
			break;
		case Type::Code::String:
			ClickHouse::add_value<ColumnString>(block, name, index, (php_type != IS_NULL) ? string_view(Z_STRVAL_P(z_value), Z_STRLEN_P(z_value)) : "", nullable, php_type == IS_NULL);
			break;
		case Type::Code::FixedString:
		{
			zend_long size = std::stol(type_param);
			if (size <= 0)
			{
				zend_error(E_WARNING, "FixedString size must be >0");
				return false;
			}

			if (php_type != IS_NULL && Z_STRLEN_P(z_value) >= static_cast<size_t>(size))
			{
				zend_error(E_WARNING, "FixedString size %lu > declared size %lu", Z_STRLEN_P(z_value), size);
				return false;
			}

			ClickHouse::add_fixed_string(block, name, index, (php_type != IS_NULL) ? string_view(Z_STRVAL_P(z_value), Z_STRLEN_P(z_value)) : "", size, nullable, php_type == IS_NULL);
			break;
		}
		case Type::Code::DateTime:
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

			ClickHouse::add_value<ColumnDateTime>(block, name, index, timestamp, nullable, php_type == IS_NULL);
			break;
		}
		case Type::Code::Date:
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

			ClickHouse::add_value<ColumnDate>(block, name, index, timestamp, nullable, php_type == IS_NULL);
			break;
		}
//		case Type::Code::Array:
		case Type::Code::Nullable:
		{
			string nested_type_param;
			Type::Code nested_type = ClickHouse::get_type(type_param, nested_type_param);

			return ClickHouse::add_by_type(block, name, index, z_value, nested_type, nested_type_param, true);
		}
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

bool ClickHouse::is_connected() const
{
	if (this->client)
		return true;

	zend_error(E_WARNING, "ClickHouse not connected");
	return false;
}

void ClickHouse::set_error(zend_long code, const char *message) const
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

void ClickHouse::set_affected_rows(zend_long value) const
{
#if PHP_API_VERSION >= 20200930
	zend_update_property_long(this->zend_this->ce, this->zend_this, "affected_rows", sizeof("affected_rows") - 1, value);
#else
	zval zv;
	ZVAL_OBJ(&zv, this->zend_this);

	zend_update_property_long(this->zend_this->ce, &zv, "affected_rows", sizeof("affected_rows") - 1, value);
#endif
}

bool ClickHouse::parse_fields(zend_array *fields, vector<zend_string*> &data)
{
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

		data.push_back(Z_STR(bucket->val));
	}
	ZEND_HASH_FOREACH_END();

	return true;
}

bool ClickHouse::parse_type(const string &text, string &type, string &param)
{
	// Parsing something like 'Nullable(String)' or 'FixedString(3)'
	const char *type_start = text.data();
	const char *type_end = type_start + text.length() - 1;

	while (*type_start == ' ')
		type_start++;
	while (*type_end == ' ')
		type_end--;

	if (type_end < type_start)
		return false;

	const char *bracket_start = strchr(type_start, '(');
	if (bracket_start == nullptr)
	{
		type.assign(type_start, type_end - type_start + 1);
		return true;
	}

	if (*type_end != ')')
		return false;

	type_end--;

	while (*type_end == ' ')
		type_end--;

	const char *param_end = bracket_start - 1;
	if (param_end < type_start)
		return false;

	while (*param_end == ' ')
		param_end--;

	if (param_end < type_start)
		return false;

	type.assign(type_start, param_end - type_start + 1);

	bracket_start++;
	while (*bracket_start == ' ')
		bracket_start++;

	if (bracket_start > type_end)
		return false;

	param.assign(bracket_start, type_end - bracket_start + 1);
	return true;
}

bool ClickHouse::parse_types(zend_array *types, vector<pair<Type::Code, string>> &data)
{
	Bucket *bucket;
	ZEND_HASH_FOREACH_BUCKET(types, bucket)
	{
		if (bucket->key != nullptr)
		{
			zend_error(E_WARNING, "Type key must be number but got string '%s'", ZSTR_VAL(bucket->key));
			return false;
		}

		if (Z_TYPE(bucket->val) != IS_STRING)
		{
			zend_error(E_WARNING, "Type must be string but got type %d", Z_TYPE(bucket->val));
			return false;
		}

		if (bucket->h != data.size())
		{
			zend_error(E_WARNING, "Type keys must go continuously in ascending order, key %lu received but %lu expected", bucket->h, data.size());
			return false;
		}

		string param;

		Type::Code type = ClickHouse::get_type(string(Z_STRVAL(bucket->val), Z_STRLEN(bucket->val)), param);
		if (type == static_cast<Type::Code>(-1))
			return false;

		data.emplace_back(type, param);
	}
	ZEND_HASH_FOREACH_END();

	return true;
}

Type::Code ClickHouse::get_type(const string &text, string &param)
{
	string type;

	if (!ClickHouse::parse_type(text, type, param))
	{
		zend_error(E_WARNING, "Wrong type '%s', bad format", text.c_str());
		return static_cast<Type::Code>(-1);
	}

	auto iter = ClickHouse::types_names.find(type);
	if (iter == ClickHouse::types_names.end())
	{
		zend_error(E_WARNING, "Type '%s' is unsupported", type.c_str());
		return static_cast<Type::Code>(-1);
	}

	bool need_param = (iter->second == Type::Code::Nullable || iter->second == Type::Code::FixedString);
	bool has_param = !param.empty();

	if (need_param != has_param)
	{
		zend_error(E_WARNING, "Nullable and FixesString types must be with parameter");
		return static_cast<Type::Code>(-1);
	}

	return iter->second;
}

Type::Code ClickHouse::get_type(zend_array *types, zend_string *name, string &param)
{
	zval *type_val = zend_hash_find(types, name);
	if (type_val == nullptr)
	{
		zend_error(E_WARNING, "Type for column '%s' not found", ZSTR_VAL(name));
		return static_cast<Type::Code>(-1);
	}

	return ClickHouse::get_type(string(Z_STRVAL_P(type_val), Z_STRLEN_P(type_val)), param);
}

zend_ulong ClickHouse::get_column_index(zend_array *names, zend_string *name)
{
	zval *index_val = zend_hash_find(names, name);
	if (index_val != nullptr)
		return Z_LVAL_P(index_val);

	zend_ulong index = zend_hash_num_elements(names);

	zval tmp;
	ZVAL_LONG(&tmp, index);

	zend_hash_add(names, name, &tmp);
	return index;
}

void ClickHouse::add_fixed_string(Block &block, zend_string *name, zend_ulong index, const string_view &value, zend_long size, bool nullable, bool is_null)
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