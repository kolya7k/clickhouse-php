#include "util.h"

static auto hex_digit(unsigned v) -> char
{
	return v < 10 ? static_cast<char>('0' + v) : static_cast<char>('a' + (v - 10));
}

auto uuid_to_string(const UUID &uuid) -> string
{
	uint64_t hi = uuid.first;
	uint64_t lo = uuid.second;

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

	put_hex((hi >> 32) & 0xffffffffULL, 8);
	put_hex((hi >> 16) & 0xffffULL,4);
	put_hex( hi	& 0xffffULL, 4);
	put_hex((lo >> 48) & 0xffffULL, 4);
	put_hex( lo	& 0xffffffffffffULL, 12);

	return out;
}

static auto hex_value(char c) -> int
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;

	return -1;
}

auto string_to_uuid(string_view text) -> std::optional<UUID>
{
	if (text.size() != 36 || text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-')
		return std::nullopt;

	uint64_t parts[2] = {0, 0};
	size_t digits = 0;

	for (char c : text)
	{
		if (c == '-')
			continue;

		int value = hex_value(c);
		if (value < 0)
			return std::nullopt;

		parts[digits / 16] = (parts[digits / 16] << 4) | static_cast<uint64_t>(value);
		digits++;
	}

	if (digits != 32)
		return std::nullopt;

	return UUID{parts[0], parts[1]};
}

auto string_to_int128(string_view text) -> std::optional<Int128>
{
	try
	{
		return Bignum::StringToInt128(text);
	}
	catch (const ValidationError&)
	{
		return std::nullopt;
	}
}

auto string_to_uint128(string_view text) -> std::optional<UInt128>
{
	try
	{
		return Bignum::StringToUInt128(text);
	}
	catch (const ValidationError&)
	{
		return std::nullopt;
	}
}

auto pow10_int64(size_t power) -> int64_t
{
	int64_t result = 1;
	for (size_t i = 0; i < power; i++)
		result *= 10;

	return result;
}