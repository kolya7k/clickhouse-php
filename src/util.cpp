#include "util.h"

namespace std
{

auto to_string(Int128 value) -> string
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

auto hex_digit(unsigned v) -> char
{
	return v < 10 ? static_cast<char>('0' + v) : static_cast<char>('a' + (v - 10));
}

auto uuid_to_string(const UUID &uuid) -> string
{
	uint64_t hi = uuid.first;
	uint64_t lo = uuid.second;

	// Стандартный формат: 8-4-4-4-12 = 36 символов
	string out(36, '0');
	int pos = 0;

	auto put_hex = [&](uint64_t value, int n) -> void
	{
		for (int i = n - 1; i >= 0; --i)
		{
			out[pos + i] = hex_digit(value & 0xF);
			value >>= 4;
		}
		pos += n;

		if (pos < 36)
			out[pos++] = '-';
	};

	// Поля UUID
	put_hex((hi >> 32) & 0xffffffffULL, 8);
	put_hex((hi >> 16) & 0xffffULL,4);
	put_hex( hi	& 0xffffULL, 4);
	put_hex((lo >> 48) & 0xffffULL, 4);
	put_hex( lo	& 0xffffffffffffULL, 12);

	return out;
}

}