#pragma once

#include <memory>
#include "irods/catalog/catalog_models.hpp"
#include "irods/irods_error.hpp"

namespace irods::catalog {

    class CatalogImpl;

    class CatalogFacade {
    public:
        CatalogFacade();
        ~CatalogFacade();

        // Data Object Operations
        irods::error register_data_object(const data_object& obj, data_id_t& out_id);
        irods::error delete_data_object(data_id_t id);

        // Replica Operations
        irods::error register_replica(const replica& repl);

        // Collection Operations
        irods::error register_collection(const collection& coll, coll_id_t& out_id);

        // ACL Operations
        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level);

    private:
        std::unique_ptr<CatalogImpl> pImpl_;
    };

} // namespace irods::catalog
