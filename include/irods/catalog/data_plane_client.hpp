#pragma once

#include <string_view>
#include <span>
#include <memory>
#include "irods/catalog/catalog_models.hpp"
#include "irods/irods_error.hpp"

namespace irods::data_plane {

    // Abstract the network/user state away from the raw rsComm_t pointer
    class SessionContext {
    public:
        virtual ~SessionContext() = default;
        virtual std::string_view client_user() const noexcept = 0;
        virtual std::string_view client_zone() const noexcept = 0;
        virtual bool is_privileged() const noexcept = 0;
    };

    // The Modern Data Plane Contract
    class Client {
    public:
        virtual ~Client() = default;

        // --- Data Object Operations ---
        
        virtual irods::error stat_data_object(SessionContext& ctx, catalog::data_id_t id, catalog::data_object& out) = 0;
        virtual irods::error stat_data_object_by_path(SessionContext& ctx, std::string_view logical_path, catalog::data_object& out) = 0;
        virtual irods::error register_data_object(SessionContext& ctx, const catalog::data_object& obj, catalog::data_id_t& out_id) = 0;
        virtual irods::error delete_data_object(SessionContext& ctx, catalog::data_id_t id) = 0;

        // --- Replica Operations ---
        virtual irods::error register_replica(SessionContext& ctx, const catalog::replica& repl) = 0;
        virtual irods::error register_replicas_bulk(SessionContext& ctx, std::span<const catalog::replica> replicas) = 0;

        // --- Transaction Management ---
        virtual irods::error begin_transaction(SessionContext& ctx) = 0;
        virtual irods::error commit_transaction(SessionContext& ctx) = 0;
        virtual irods::error rollback_transaction(SessionContext& ctx) = 0;
    };

    // RAII Transaction Guard
    class TransactionGuard {
    public:
        TransactionGuard(Client& db, SessionContext& ctx) : db_(db), ctx_(ctx) {
            db_.begin_transaction(ctx_);
        }
        ~TransactionGuard() {
            if (!committed_) {
                db_.rollback_transaction(ctx_);
            }
        }
        void commit() {
            if (db_.commit_transaction(ctx_).ok()) {
                committed_ = true;
            }
        }
    private:
        Client& db_;
        SessionContext& ctx_;
        bool committed_ = false;
    };

} // namespace irods::data_plane
