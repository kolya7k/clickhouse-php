#include "util.h"

namespace std
{

string to_string(clickhouse::Int128 value)
{
	if (value == 0)
		return {"0"};

	bool negative = value < 0;
	if (negative)
		value *= -1;

	char buffer[64] = {0};
	char *end = buffer + sizeof(buffer) - 1;
	*end = '\0';

	while (value != 0)
	{
		if (end == buffer)
			return {};

		*--end = "0123456789"[static_cast<int>(value % 10)];
		value /= 10;
	}

	if (negative)
		*--end = '-';

	return {end};
}

}