#include "core/config/name_table.h"

namespace axc::core
{

void NameTable::add_honoured(std::string_view name)
{
    add(name, DispositionKind::honoured, {});
}

void NameTable::add_accepted_ignored(std::string_view name, std::string_view reason)
{
    add(name, DispositionKind::accepted_ignored, reason);
}

void NameTable::add_refused(std::string_view name, std::string_view reason)
{
    add(name, DispositionKind::refused, reason);
}

void NameTable::add(std::string_view name, DispositionKind kind, std::string_view reason)
{
    Disposition disposition;
    disposition.kind = kind;
    disposition.reason.assign(reason);
    entries_[std::string{name}] = disposition;
}

Disposition NameTable::lookup(std::string_view name) const noexcept
{
    const auto entry = entries_.find(name);
    return entries_.end() == entry ? Disposition{} : entry->second;
}

std::vector<std::string> NameTable::unknown_names(const PropertySource& source) const
{
    std::vector<std::string> names;
    source.for_each(
        [this, &names](const std::string& name, const std::string&)
        {
            if (!lookup(name).is_known())
            {
                names.push_back(name);
            }
        });
    return names;
}

std::vector<std::string> NameTable::refused_names(const PropertySource& source) const
{
    std::vector<std::string> names;
    source.for_each(
        [this, &names](const std::string& name, const std::string&)
        {
            if (DispositionKind::refused == lookup(name).kind)
            {
                names.push_back(name);
            }
        });
    return names;
}

}  // namespace axc::core
