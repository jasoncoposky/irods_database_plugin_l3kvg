#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <cstring>
#include <vector>

#define XXH_INLINE_ALL
#include "xxhash.h"
#include "L3KVG/FederationID.hpp"

namespace irods::catalog {

    enum class EntityType : uint8_t {
        Zone = 0x01,
        User = 0x02,
        Collection = 0x03,
        DataObject = 0x04,
        Replica = 0x05,
        Resource = 0x06,
        Metadata = 0x07,
        Access = 0x08,
        Rule = 0x09,
        Token = 0x0A,
        Quota = 0x0B,
        Audit = 0x0C,
        Ticket = 0x0D
    };

    /**
     * L3KVG Snowflake ID (64-bit)
     * 
     * [ 16 bits: Cluster ID ] [ 48 bits: Local Node Hash ]
     */
    using snowflake_id_t = uint64_t;

    struct SnowflakeID {
        static constexpr uint64_t LOCAL_HASH_MASK = l3kvg::FederationID::LOCAL_HASH_MASK;

        static uint16_t calculate_cluster_id(std::string_view zone_name) {
            // Use XXH32 for the 16-bit mapping to ensure determinism across the grid
            return static_cast<uint16_t>(XXH32(zone_name.data(), zone_name.size(), 0) & 0xFFFF);
        }

        static snowflake_id_t create(uint16_t cluster_id, std::string_view local_uuid) {
            uint64_t hash = XXH3_64bits(local_uuid.data(), local_uuid.size());
            return l3kvg::FederationID::pack(cluster_id, hash);
        }

        static snowflake_id_t create_from_number(uint16_t cluster_id, uint64_t numeric_id) {
             // For numeric IDs (like irods native IDs), we still hash them into 48 bits 
             // to ensure uniform distribution across shards while preserving the cluster prefix.
             std::string s = std::to_string(numeric_id);
             return create(cluster_id, s);
        }

        static uint16_t get_cluster_id(snowflake_id_t id) {
            return l3kvg::FederationID::get_cluster(id);
        }

        static uint64_t get_local_hash(snowflake_id_t id) {
            return l3kvg::FederationID::get_local_hash(id);
        }

        static std::string pack(snowflake_id_t id) {
            // L3KVG Engine still accepts string keys at the API boundary,
            // so we pack the 8-byte integer into an 8-byte string.
            std::string packed;
            packed.resize(8);
            std::memcpy(packed.data(), &id, 8);
            return packed;
        }

        static snowflake_id_t unpack(std::string_view packed) {
            if (packed.size() < 8) return 0;
            snowflake_id_t id;
            std::memcpy(&id, packed.data(), 8);
            return id;
        }
    };

} // namespace irods::catalog
