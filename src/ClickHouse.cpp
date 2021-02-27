#include "ClickHouse.h"

ClickHouse::ClickHouse(zval *zend_this, zend_string *host, zend_string *username, zend_string *passwd, zend_string *dbname, zend_long port):
	zend_this(zend_this)
{
	if (host != nullptr)
		this->host = string(ZSTR_VAL(host), ZSTR_LEN(host));
	if (username != nullptr)
		this->username = string(ZSTR_VAL(username), ZSTR_LEN(username));
	if (passwd != nullptr)
		this->passwd = string(ZSTR_VAL(passwd), ZSTR_LEN(passwd));
	if (dbname != nullptr)
		this->dbname = string(ZSTR_VAL(dbname), ZSTR_LEN(dbname));
	if (port != 0)
		this->port = port;

//	zend_printf("ClickHouse host '%s', username '%s', passwd '%s', dbname '%s', port %u\n", this->host.c_str(), this->username.c_str(), this->passwd.c_str(), this->dbname.c_str(), this->port);
}

ClickHouse::~ClickHouse()
{
/*
	zval tmp;
	zval *zv_errno = zend_read_property(Z_OBJCE_P(this->zend_this), this->zend_this, "errno", sizeof("errno") - 1, 1, &tmp);
	if (zv_errno != nullptr)
		zend_string_release(Z_STR_P(zv_errno));
*/
}

void ClickHouse::connect()
{
	ClientOptions options;
	options.SetHost(this->host);
	options.SetUser(this->username);
	options.SetPassword(this->passwd);
	options.SetDefaultDatabase(this->dbname);
	options.SetPort(this->port);

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

ClickHouseResult* ClickHouse::query(const string &query)
{
	string trimmed = ltrim(query);

	bool is_select = trimmed.length() >= SELECT.length() && strncasecmp(trimmed.c_str(), SELECT.c_str(), SELECT.length()) == 0;
	if (is_select)
		return this->select(query);

	bool is_insert = trimmed.length() >= INSERT.length() && strncasecmp(trimmed.c_str(), INSERT.c_str(), INSERT.length()) == 0;
	if (is_insert)
		return this->insert(query);

	return this->execute(query);
}

ClickHouseResult* ClickHouse::select(const string &query)
{
	if (!this->is_connected())
		return nullptr;

	ClickHouseResult::blocks_t blocks;

	try
	{
		this->client->Select(query, [&blocks] (const Block &block)
		{
			blocks.push_back(block);
		});
	}
	catch (ServerException &e)
	{
		this->set_error(e.GetCode(), e.what());
		return nullptr;
	}

	ClickHouseResult *result = new ClickHouseResult(blocks);

	return result;
}

ClickHouseResult* ClickHouse::insert(const string &query)
{
	if (!this->is_connected())
		return nullptr;

	return nullptr;
}

ClickHouseResult* ClickHouse::execute(const string &query)
{
	if (!this->is_connected())
		return nullptr;

	return nullptr;
}

bool ClickHouse::is_connected() const
{
	if (this->client)
		return true;

	zend_error(E_WARNING, "ClickHouse not connected");
	return false;
}

void ClickHouse::set_error(uint32_t code, const char *message) const
{
#if PHP_API_VERSION >= 20200930
	zend_update_property_long(Z_OBJCE_P(this->zend_this), Z_OBJ_P(this->zend_this), "errno", sizeof("errno") - 1, code);
	zend_update_property_string(Z_OBJCE_P(this->zend_this), Z_OBJ_P(this->zend_this), "error", sizeof("error") - 1, message);
#else
	zend_update_property_long(Z_OBJCE_P(this->zend_this), this->zend_this, "errno", sizeof("errno") - 1, code);
	zend_update_property_string(Z_OBJCE_P(this->zend_this), this->zend_this, "error", sizeof("error") - 1, message);
#endif
}