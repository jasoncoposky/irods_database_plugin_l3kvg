#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

namespace irods::catalog {

    /**
     * FederationResolver manages the mapping between 16-bit Cluster IDs 
     * and their respective ZeroMQ endpoints.
     */
    class FederationResolver {
    public:
        void register_local_cluster(const std::string& name, uint16_t id) {
            std::lock_guard<std::mutex> lock(mu_);
            local_id_ = id;
            cluster_names_[id] = name;
        }

        void register_remote_cluster(const std::string& name, uint16_t id, const std::string& endpoint) {
            std::lock_guard<std::mutex> lock(mu_);
            cluster_endpoints_[id] = endpoint;
            cluster_names_[id] = name;
        }

        bool is_local(uint16_t cluster_id) const {
            return cluster_id == local_id_;
        }

        std::string get_endpoint(uint16_t cluster_id) const {
            auto it = cluster_endpoints_.find(cluster_id);
            if (it != cluster_endpoints_.end()) return it->second;
            return "";
        }

        uint16_t get_local_id() const { return local_id_; }

    private:
        uint16_t local_id_ = 0;
        std::unordered_map<uint16_t, std::string> cluster_endpoints_;
        std::unordered_map<uint16_t, std::string> cluster_names_;
        mutable std::mutex mu_;
    };

} // namespace irods::catalog
