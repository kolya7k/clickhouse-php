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
//	{"Nullable", Type::Code::Nullable},
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

	vector<Type::Code> types_data;
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
				type = types_data[index];
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
				type = ClickHouse::get_type(types, name);
			}

			if (type == static_cast<Type::Code>(-1))
			{
				zend_array_destroy(Z_ARR(column_names));
				return false;
			}

			auto php_type = Z_TYPE(value_bucket->val);
			bool types_match = true;

			switch (type)
			{
//				case Type::Code::Void:
				case Type::Code::Int8:
				case Type::Code::Int16:
				case Type::Code::Int32:
				case Type::Code::Int64:
				case Type::Code::UInt8:
				case Type::Code::UInt16:
				case Type::Code::UInt32:
				case Type::Code::UInt64:
					types_match = (php_type == IS_LONG);
					break;
				case Type::Code::Float32:
				case Type::Code::Float64:
					types_match = (php_type == IS_DOUBLE);
					break;
				case Type::Code::String:
					types_match = (php_type == IS_STRING);
					break;
				case Type::Code::DateTime:
				case Type::Code::Date:
					types_match = (php_type == IS_STRING || php_type == IS_LONG);
					break;
//				case Type::Code::Array:
				case Type::Code::Nullable:
					// TODO: Check nested column
//					types_match = (php_type == IS_NULL);
					break;
//				case Type::Code::Tuple:
//				case Type::Code::Enum8:
//				case Type::Code::Enum16:
//				case Type::Code::UUID:
//				case Type::Code::IPv4:
//				case Type::Code::IPv6:
//				case Type::Code::Int128:
//				case Type::Code::Decimal:
//				case Type::Code::Decimal32:
//				case Type::Code::Decimal64:
//				case Type::Code::Decimal128:
//				case Type::Code::LowCardinality:
				default:
					zend_error(E_WARNING, "Value type %d is unsupported", type);
					zend_array_destroy(Z_ARR(column_names));
					return false;
			}

			if (!types_match)
			{
				zend_error(E_WARNING, "Value type and described type mismatch for row %lu and value '%s'", values_bucket->h, ZSTR_VAL(name));
				zend_array_destroy(Z_ARR(column_names));
				return false;
			}

			switch (type)
			{
//				case Type::Code::Void:
				case Type::Code::Int8:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnInt8>(block, name, index);
					else
						ClickHouse::add_long<ColumnInt8>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::Int16:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnInt16>(block, name, index);
					else
						ClickHouse::add_long<ColumnInt16>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::Int32:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnInt32>(block, name, index);
					else
						ClickHouse::add_long<ColumnInt32>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::Int64:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnInt64>(block, name, index);
					else
						ClickHouse::add_long<ColumnInt64>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::UInt8:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnUInt8>(block, name, index);
					else
						ClickHouse::add_long<ColumnUInt8>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::UInt16:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnUInt16>(block, name, index);
					else
						ClickHouse::add_long<ColumnUInt16>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::UInt32:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnUInt32>(block, name, index);
					else
						ClickHouse::add_long<ColumnUInt32>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::UInt64:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnUInt64>(block, name, index);
					else
						ClickHouse::add_long<ColumnUInt64>(block, name, index, Z_LVAL(value_bucket->val));
					break;
				case Type::Code::Float32:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnFloat32>(block, name, index);
					else
						ClickHouse::add_float<ColumnFloat32>(block, name, index, Z_DVAL(value_bucket->val));
					break;
				case Type::Code::Float64:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnFloat64>(block, name, index);
					else
						ClickHouse::add_float<ColumnFloat64>(block, name, index, Z_DVAL(value_bucket->val));
					break;
				case Type::Code::String:
					if (php_type == IS_NULL)
						ClickHouse::add_null<ColumnString>(block, name, index);
					else
						ClickHouse::add_string(block, name, index, string_view(Z_STRVAL(value_bucket->val), Z_STRLEN(value_bucket->val)));
					break;
				case Type::Code::FixedString:
//					if (php_type == IS_NULL)
//						ClickHouse::add_null<ColumnFixedString>(block, name, index);
//					else
						ClickHouse::add_fixed_string(block, name, index, string_view(Z_STRVAL(value_bucket->val), Z_STRLEN(value_bucket->val)));
					break;
				case Type::Code::DateTime:
				{
					if (php_type == IS_NULL)
					{
						ClickHouse::add_null<ColumnDateTime>(block, name, index);
						break;
					}

					time_t timestamp;
					if (php_type == IS_LONG)
						timestamp = Z_LVAL(value_bucket->val);
					else
					{
						tm tm_time{};
						if (strptime(Z_STRVAL(value_bucket->val), "%Y-%m-%d %H:%M:%S", &tm_time) == nullptr)
						{
							zend_error(E_WARNING, "Failed to parse date '%s' from format '%%Y-%%m-%%d %%H:%%M:%%S'", Z_STRVAL(value_bucket->val));
							zend_array_destroy(Z_ARR(column_names));
							return false;
						}

						timestamp = mktime(&tm_time);
					}

					ClickHouse::add_datetime(block, name, index, timestamp);
					break;
				}
				case Type::Code::Date:
				{
					if (php_type == IS_NULL)
					{
						ClickHouse::add_null<ColumnDate>(block, name, index);
						break;
					}

					time_t timestamp;
					if (php_type == IS_LONG)
						timestamp = Z_LVAL(value_bucket->val) * (24 * 60 * 60);
					else
					{
						tm tm_time{};
						if (strptime(Z_STRVAL(value_bucket->val), "%Y-%m-%d", &tm_time) == nullptr)
						{
							zend_error(E_WARNING, "Failed to parse date '%s' from format '%%Y-%%m-%%d'", Z_STRVAL(value_bucket->val));
							zend_array_destroy(Z_ARR(column_names));
							return false;
						}

						timestamp = timegm(&tm_time);
					}

					ClickHouse::add_date(block, name, index, timestamp);
					break;
				}
//				case Type::Code::Array:
				case Type::Code::Nullable:
//				case Type::Code::Tuple:
//				case Type::Code::Enum8:
//				case Type::Code::Enum16:
//				case Type::Code::UUID:
//				case Type::Code::IPv4:
//				case Type::Code::IPv6:
//				case Type::Code::Int128:
//				case Type::Code::Decimal:
//				case Type::Code::Decimal32:
//				case Type::Code::Decimal64:
//				case Type::Code::Decimal128:
//				case Type::Code::LowCardinality:
				default:
					zend_error(E_WARNING, "Value type %d is unsupported", type);
					zend_array_destroy(Z_ARR(column_names));
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
		return false;
	}

	this->set_affected_rows(rows);
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
	zend_update_property_long(zend_this->ce, this->zend_this, "errno", sizeof("errno") - 1, code);
	zend_update_property_string(zend_this->ce, this->zend_this, "error", sizeof("error") - 1, message);
#else
	zval zv;
	ZVAL_OBJ(&zv, zend_this);

	zend_update_property_long(zend_this->ce, &zv, "errno", sizeof("errno") - 1, code);
	zend_update_property_string(zend_this->ce, &zv, "error", sizeof("error") - 1, message);
#endif
}

void ClickHouse::set_affected_rows(zend_long value) const
{
#if PHP_API_VERSION >= 20200930
	zend_update_property_long(zend_this->ce, zend_this, "affected_rows", sizeof("affected_rows") - 1, value);
#else
	zval zv;
	ZVAL_OBJ(&zv, zend_this);

	zend_update_property_long(zend_this->ce, &zv, "affected_rows", sizeof("affected_rows") - 1, value);
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

bool ClickHouse::parse_types(zend_array *types, vector<Type::Code> &data)
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

		string type(Z_STRVAL(bucket->val), Z_STRLEN(bucket->val));

		auto iter = ClickHouse::types_names.find(type);
		if (iter == ClickHouse::types_names.end())
		{
			zend_error(E_WARNING, "Type '%s' is unsupported", type.c_str());
			return false;
		}

		data.push_back(iter->second);
	}
	ZEND_HASH_FOREACH_END();

	return true;
}

Type::Code ClickHouse::get_type(zend_array *types, zend_string *name)
{
	zval *type_val = zend_hash_find(types, name);
	if (type_val == nullptr)
	{
		zend_error(E_WARNING, "Type for column '%s' not found", ZSTR_VAL(name));
		return static_cast<Type::Code>(-1);
	}

	string type(Z_STRVAL_P(type_val), Z_STRLEN_P(type_val));

	auto iter = ClickHouse::types_names.find(type);
	if (iter == ClickHouse::types_names.end())
	{
		zend_error(E_WARNING, "Unsupported type '%s'", type.c_str());
		return static_cast<Type::Code>(-1);
	}

	return iter->second;
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

void ClickHouse::add_string(Block &block, zend_string *name, zend_ulong index, const string_view &value)
{
	if (block.GetColumnCount() <= index)
		block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnString>());

	block[index]->As<ColumnString>()->Append(value);
}

void ClickHouse::add_fixed_string(Block &block, zend_string *name, zend_ulong index, const string_view &value)
{
	if (block.GetColumnCount() <= index)
		block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnFixedString>(value.length()));

	block[index]->As<ColumnFixedString>()->Append(value);
}

void ClickHouse::add_date(Block &block, zend_string *name, zend_ulong index, time_t time)
{
	if (block.GetColumnCount() <= index)
		block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnDate>());

	block[index]->As<ColumnDate>()->Append(time);
}

void ClickHouse::add_datetime(Block &block, zend_string *name, zend_ulong index, time_t time)
{
	if (block.GetColumnCount() <= index)
		block.AppendColumn(string(ZSTR_VAL(name), ZSTR_LEN(name)), make_shared<ColumnDateTime>());

	block[index]->As<ColumnDateTime>()->Append(time);
}