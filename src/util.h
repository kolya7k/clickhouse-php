#pragma once

#include <optional>

[[nodiscard]] auto uuid_to_string(const UUID &uuid) -> string;
[[nodiscard]] auto string_to_uuid(string_view text) -> std::optional<UUID>;
[[nodiscard]] auto string_to_int128(string_view text) -> std::optional<Int128>;
[[nodiscard]] auto string_to_uint128(string_view text) -> std::optional<UInt128>;
[[nodiscard]] auto pow10_int64(size_t power) -> int64_t;