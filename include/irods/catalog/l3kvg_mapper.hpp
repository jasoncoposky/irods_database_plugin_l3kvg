#pragma once

#include <string_view>
#include <tuple>
#include <type_traits>
#include "buffer.hpp"

namespace irods::l3kvg_mapper {

    // Helper to safely view legacy C-strings or char arrays without copying
    inline std::string_view zero_copy(const char* c_str) noexcept {
        return (c_str == nullptr) ? std::string_view{} : std::string_view{c_str};
    }

    // The Compile-Time Schema Descriptor
    template <typename LegacyStruct, typename FieldType>
    struct FieldMap {
        const char* bson_key;
        FieldType LegacyStruct::* member_ptr;
    };

    // Deduction guide
    template <typename T, typename U>
    FieldMap(const char*, U T::*) -> FieldMap<T, U>;

    // Helper to extract the value and apply zero-copy if it's a character array
    template <typename T>
    decltype(auto) extract_value(const T& val) {
        if constexpr (std::is_array_v<T>) {
            return zero_copy(reinterpret_cast<const char*>(val));
        } else if constexpr (std::is_pointer_v<T>) {
            if constexpr (std::is_same_v<std::remove_cv_t<std::remove_pointer_t<T>>, char>) {
                return zero_copy(reinterpret_cast<const char*>(val));
            } else {
                return val;
            }
        } else {
            return val;
        }
    }

    // The Engine: Folds over the tuple to build the BSON in lite3cpp::Buffer
    template <typename LegacyStruct, typename Tuple, std::size_t... Is>
    void build_bson_impl(lite3cpp::Buffer& buf, size_t ofs, const LegacyStruct& obj, const Tuple& schema, std::index_sequence<Is...>) {
        (..., ([&]() {
            constexpr auto field = std::get<Is>(schema);
            auto val = extract_value(obj.*(field.member_ptr));
            using V = decltype(val);

            if constexpr (std::is_same_v<V, std::string_view>) {
                buf.set_str(ofs, field.bson_key, val);
            } else if constexpr (std::is_integral_v<V>) {
                buf.set_i64(ofs, field.bson_key, static_cast<int64_t>(val));
            } else if constexpr (std::is_floating_point_v<V>) {
                buf.set_f64(ofs, field.bson_key, static_cast<double>(val));
            }
        }()));
    }

    // The Public API for serializing a struct to a Buffer
    template <typename LegacyStruct, typename Tuple>
    lite3cpp::Buffer to_buffer(const LegacyStruct& obj, const Tuple& schema) {
        lite3cpp::Buffer buf;
        buf.init_object();
        build_bson_impl(buf, 0, obj, schema, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
        return buf;
    }

    // Returns a view of the buffer data to avoid intermediate std::string allocations
    inline std::string_view to_view(const lite3cpp::Buffer& buf) noexcept {
        return std::string_view{reinterpret_cast<const char*>(buf.data()), buf.size()};
    }

} // namespace irods::l3kvg_mapper
