#pragma once

namespace std
{
	auto to_string(Int128 value) -> string;
	auto uuid_to_string(const UUID &uuid) -> string;
}