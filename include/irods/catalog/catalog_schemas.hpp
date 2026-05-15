#pragma once

#include <tuple>
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/objInfo.h"

namespace irods::catalog::schema {

    using namespace irods::l3kvg_mapper;

    // The single source of truth for mapping a logical Data Object Node
    constexpr auto DATA_OBJECT_SCHEMA = std::make_tuple(
        FieldMap{"id",          &dataObjInfo_t::dataId},
        FieldMap{"coll_id",     &dataObjInfo_t::collId},
        FieldMap{"name",        &dataObjInfo_t::objPath},
        FieldMap{"size",        &dataObjInfo_t::dataSize},
        FieldMap{"owner",       &dataObjInfo_t::dataOwnerName},
        FieldMap{"owner_zone",  &dataObjInfo_t::dataOwnerZone},
        FieldMap{"create_ts",   &dataObjInfo_t::dataCreate},
        FieldMap{"modify_ts",   &dataObjInfo_t::dataModify},
        FieldMap{"checksum",    &dataObjInfo_t::chksum},
        FieldMap{"data_type",   &dataObjInfo_t::dataType},
        FieldMap{"repl_num",    &dataObjInfo_t::replNum},
        FieldMap{"status",      &dataObjInfo_t::statusString},
        FieldMap{"path",        &dataObjInfo_t::filePath},
        FieldMap{"resc_hier",   &dataObjInfo_t::rescHier},
        FieldMap{"resc_id",     &dataObjInfo_t::rescId}
    );

    // Schema for Collection Node
    constexpr auto COLLECTION_SCHEMA = std::make_tuple(
        FieldMap{"id",          &collInfo_t::collId},
        FieldMap{"name",        &collInfo_t::collName},
        FieldMap{"owner",       &collInfo_t::collOwnerName},
        FieldMap{"owner_zone",  &collInfo_t::collOwnerZone},
        FieldMap{"create_ts",   &collInfo_t::collCreate},
        FieldMap{"modify_ts",   &collInfo_t::collModify},
        FieldMap{"inheritance", &collInfo_t::collInheritance}
    );

    // Schema for User Node
    constexpr auto USER_SCHEMA = std::make_tuple(
        FieldMap{"name",        &userInfo_t::userName},
        FieldMap{"zone",        &userInfo_t::rodsZone},
        FieldMap{"type",        &userInfo_t::userType}
    );

} // namespace irods::catalog::schema
