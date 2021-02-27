#pragma once

#include "ClickHouseResult.h"

class ClickHouse
{
private:
	inline static const string DEFAULT_HOST = "127.0.0.1";
	inline static const string DEFAULT_USERNAME = "default";
	inline static const string DEFAULT_PASSWD;
	inline static const string DEFAULT_DBNAME = "default";
	inline static const uint32_t DEFAULT_PORT = 9000;

	inline static const string SELECT = "SELECT";
	inline static const string INSERT = "INSERT";

	zval *zend_this;
	shared_ptr<Client> client;

	[[nodiscard]] bool is_connected() const;
	void set_error(uint32_t code, const char *message) const;

public:
	string host = DEFAULT_HOST;
	string username = DEFAULT_USERNAME;
	string passwd = DEFAULT_PASSWD;
	string dbname = DEFAULT_DBNAME;
	uint32_t port = DEFAULT_PORT;

	ClickHouse(zval *zend_this, zend_string *host, zend_string *username, zend_string *passwd, zend_string *dbname, zend_long port);
	~ClickHouse();

	void connect();

	ClickHouseResult* query(const string &query);
	ClickHouseResult* select(const string &query);
	ClickHouseResult* insert(const string &query);
	ClickHouseResult* execute(const string &query);
};