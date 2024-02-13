#include <arpa/inet.h>
#include "ipaddress.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

IPAddress::IPAddress() : type(IPAddressType::IP_ADDRESS_INVALID) {
}

IPAddress::IPAddress(IPAddressType type, hugeint_t address, uint16_t mask) : type(type), address(address), mask(mask) {
}

IPAddress IPAddress::FromIPv4(int32_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V4, address, mask);
}
IPAddress IPAddress::FromIPv6(hugeint_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V6, address, mask);
}

static bool IPAddressError(string_t input, string *error_message, string error) {
	string e = "Failed to convert string \"" + input.GetString() + "\" to inet: " + error;
	HandleCastError::AssignError(e, error_message);
	return false;
}

bool IPAddress::TryParse(string_t input, IPAddress &result, string *error_message) {
	auto data = input.GetData();
	auto size = input.GetSize();
	idx_t c = 0;
	idx_t number_count = 0;

	if (inet_pton(AF_INET, data, &result.address)) {
		result.type = IPAddressType::IP_ADDRESS_V4;
		result.mask = IPAddress::IPV4_DEFAULT_MASK;
	}
	else if (inet_pton(AF_INET6, data, &result.address)) {
		result.type = IPAddressType::IP_ADDRESS_V6;
		result.mask = IPAddress::IPV6_DEFAULT_MASK;
	}
	else {
		return IPAddressError(input, error_message, "Failed to parse IP address");
	}

	for (c = 0; c < size; c++) {
		if (data[c] == '/')
			break;
	}

	if (c == size) {
		// no mask, use default for address family
		return true;
	}

	c++;
	idx_t start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	uint8_t mask;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), mask)) {
		return IPAddressError(input, error_message, "Faied to parse IP network mask");
	}
	if (mask > result.mask) {
		return IPAddressError(input, error_message, "Expected a number between 0-32 for IPv4 and 0-128 for IPv6");
	}
	result.mask = mask;
	return true;
}

string IPAddress::ToString() const {
	string result;
	char buffer[INET6_ADDRSTRLEN];

	inet_ntop(type == IPAddressType::IP_ADDRESS_V4 ? AF_INET : AF_INET6, &address, buffer, INET6_ADDRSTRLEN);
	result.append(buffer);

	if ((type == IPAddressType::IP_ADDRESS_V4 && mask != IPAddress::IPV4_DEFAULT_MASK) ||
	    (type == IPAddressType::IP_ADDRESS_V6 && mask != IPAddress::IPV6_DEFAULT_MASK)) {
		result += "/" + to_string(mask);
	}
	return result;
}

IPAddress IPAddress::FromString(string_t input) {
	IPAddress result;
	string error_message;
	if (!TryParse(input, result, &error_message)) {
		throw ConversionException(error_message);
	}
	return result;
}

} // namespace duckdb
