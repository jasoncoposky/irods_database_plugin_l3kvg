#pragma once

#include <tuple>
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/objInfo.h"

namespace irods::catalog::schema {

    using namespace irods::l3kvg_mapper;

    // The single source of truth for mapping a logical Data Object Node
    constexpr auto DATA_OBJECT_SCHEMA = std::make_tuple(
        FieldMap{"id",          &dataObjInfo_t::dataId},
        FieldMap{"name",        &dataObjInfo_t::objPath},
        FieldMap{"size",        &dataObjInfo_t::dataSize},
        FieldMap{"owner",       &dataObjInfo_t::dataOwnerName},
        FieldMap{"create_ts",   &dataObjInfo_t::dataCreate},
        FieldMap{"modify_ts",   &dataObjInfo_t::dataModify}
    );


    // Schema for the physical replica properties on the REPLICATED_ON edge
    constexpr auto REPLICA_EDGE_SCHEMA = std::make_tuple(
        FieldMap{"path",        &dataObjInfo_t::filePath},
        FieldMap{"repl_num",    &dataObjInfo_t::replNum},
        FieldMap{"resc_name",   &dataObjInfo_t::rescName},
        FieldMap{"status",      &dataObjInfo_t::statusString},
        FieldMap{"chksum",      &dataObjInfo_t::chksum}
    );

    // Schema for Collection Node
    constexpr auto COLLECTION_SCHEMA = std::make_tuple(
        FieldMap{"id",          &collInfo_t::collId},
        FieldMap{"name",        &collInfo_t::collName},
        FieldMap{"owner",       &collInfo_t::collOwnerName},
        FieldMap{"owner_zone",  &collInfo_t::collOwnerZone},
        FieldMap{"create_ts",   &collInfo_t::collCreate},
        FieldMap{"modify_ts",   &collInfo_t::collModify}
    );

} // namespace irods::catalog::schema
