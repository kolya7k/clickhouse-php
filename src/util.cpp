#include "util.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
string int128_to_string(__int128 value)
{
	if (value == 0)
		return string("0");

	char buffer[40] = {0};
	char *end = buffer + sizeof(buffer) - 1;
	*end = '\0';

	while (value != 0)
	{
		if (end == buffer)
			return string();

		*--end = "0123456789"[value % 10];
		value /= 10;
	}

	return string(end);
}
#pragma GCC diagnostic pop