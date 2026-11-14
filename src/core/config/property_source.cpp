#include "core/config/property_source.h"

#include <cctype>
#include <cstdio>
#include <fstream>
#include <string>

extern char** environ;

namespace axc::core
{
namespace
{

std::string_view trim(std::string_view text) noexcept
{
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0)
    {
        text.remove_prefix(1);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())) != 0)
    {
        text.remove_suffix(1);
    }
    return text;
}

}  // namespace

std::expected<PropertySource, Error> PropertySource::from_args(int argc, const char* const argv[]) noexcept
{
    PropertySource source;

    for (int index = 1; index < argc; ++index)
    {
        const std::string_view argument{argv[index]};
        if (argument.size() < 3 || argument.substr(0, 2) != "-D")
        {
            continue;
        }

        const std::string_view assignment = argument.substr(2);
        const std::size_t equals = assignment.find('=');
        if (std::string_view::npos == equals)
        {
            source.set(assignment, std::string_view{});
        }
        else
        {
            source.set(assignment.substr(0, equals), assignment.substr(equals + 1));
        }
    }

    return source;
}

std::expected<void, Error> PropertySource::load_file(std::string_view path) noexcept
{
    std::ifstream file{std::string{path}};
    if (!file.is_open())
    {
        return std::unexpected(make_error(ErrorCode::io_error, "cannot open properties file"));
    }

    std::string line;
    int line_number = 0;
    while (std::getline(file, line))
    {
        ++line_number;
        const std::string_view trimmed = trim(line);
        if (trimmed.empty() || '#' == trimmed.front() || '!' == trimmed.front())
        {
            continue;
        }

        const std::size_t equals = trimmed.find('=');
        if (std::string_view::npos == equals)
        {
            char message[128];
            std::snprintf(message, sizeof(message), "properties line %d has no '='", line_number);
            return std::unexpected(make_error(ErrorCode::config_error, message));
        }

        set_if_absent(trim(trimmed.substr(0, equals)), trim(trimmed.substr(equals + 1)));
    }

    return {};
}

std::expected<void, Error> PropertySource::load_environment(std::string_view prefix,
                                                            std::string_view name_prefix) noexcept
{
    if (nullptr == environ)
    {
        return {};
    }

    for (const char* const* entry = environ; *entry != nullptr; ++entry)
    {
        const std::string_view variable{*entry};
        const std::size_t equals = variable.find('=');
        if (std::string_view::npos == equals || equals < prefix.size() || variable.substr(0, prefix.size()) != prefix)
        {
            continue;
        }

        const std::string_view suffix = variable.substr(prefix.size(), equals - prefix.size());
        std::string name{name_prefix};
        for (const char c : suffix)
        {
            name.push_back('_' == c ? '.' : static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }

        set_if_absent(name, variable.substr(equals + 1));
    }

    return {};
}

void PropertySource::set(std::string_view name, std::string_view value)
{
    values_[std::string{name}] = std::string{value};
}

void PropertySource::set_if_absent(std::string_view name, std::string_view value)
{
    if (!contains(name))
    {
        set(name, value);
    }
}

std::optional<std::string_view> PropertySource::get(std::string_view name) const noexcept
{
    const auto entry = values_.find(name);
    if (values_.end() == entry)
    {
        return std::nullopt;
    }
    return std::string_view{entry->second};
}

std::string PropertySource::get_or(std::string_view name, std::string_view fallback) const
{
    const auto value = get(name);
    return std::string{value.has_value() ? *value : fallback};
}

bool PropertySource::contains(std::string_view name) const noexcept
{
    return values_.end() != values_.find(name);
}

}  // namespace axc::core
