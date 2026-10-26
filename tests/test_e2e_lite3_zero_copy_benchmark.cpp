#include "plugin_test_fixture.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/private/genquery2_sql.hpp"
#include "irods/genquery1_ast_adapter.hpp"
#include "irods/atomic_apply_database_operations.hpp"
#include "irods/protocol/lite3_structs.hpp"
#include "irods/rodsGenQuery.h"
#include "buffer.hpp"
#include "json.hpp"
#include <nlohmann/json.hpp>

#include <chrono>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>

using namespace irods::catalog;
using namespace irods::catalog::test;
namespace gq2 = irods::experimental::genquery2;
namespace dml = irods::experimental::dml;

class EndToEndLite3ZeroCopyBenchmarkTest : public PluginTestFixture {};

TEST_F(EndToEndLite3ZeroCopyBenchmarkTest, AtomicApplyAndZeroCopyQueryPipeline) {
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test_e2e_bench.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    ASSERT_TRUE(plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr).ok());
    ASSERT_TRUE(plugin()->has_operation(irods::DATABASE_OP_EXECUTE_GENQUERY2_BUFFER));
    ASSERT_TRUE(plugin()->has_operation(irods::DATABASE_OP_ATOMIC_APPLY));

    // 1. Register collection and batch-insert 10 data objects via DATABASE_OP_ATOMIC_APPLY
    collInfo_t coll{};
    coll.collId = 500;
    std::snprintf(coll.collName, sizeof(coll.collName), "/tempZone/home/rods/bench_coll");
    std::snprintf(coll.collOwnerName, sizeof(coll.collOwnerName), "rods");
    std::snprintf(coll.collOwnerZone, sizeof(coll.collOwnerZone), "tempZone");
    ASSERT_TRUE((plugin()->call<collInfo_t*>(nullptr, irods::DATABASE_OP_REG_COLL, nullptr, &coll).ok()));

    std::vector<dml::operation_type> batch_ops;
    for (int i = 0; i < 10; ++i) {
        batch_ops.emplace_back(dml::insert_op{
            "R_DATA_MAIN",
            {
                {"data_id", std::to_string(10000 + i)},
                {"coll_id", "500"},
                {"data_name", "bench_file_" + std::to_string(i) + ".bin"},
                {"data_size", std::to_string(4096 * (i + 1))},
                {"data_owner_name", "rods"},
                {"data_owner_zone", "tempZone"},
                {"data_is_dirty", "1"}
            }
        });
    }

    // Update bench_file_0.bin size in the same atomic batch
    batch_ops.emplace_back(dml::update_op{
        "R_DATA_MAIN",
        {{"data_size", "65536"}},
        {dml::condition{"data_name", "=", {"bench_file_0.bin"}}}
    });

    auto batch_ret = plugin()->call<const std::vector<dml::operation_type>*>(
        nullptr, irods::DATABASE_OP_ATOMIC_APPLY, nullptr, &batch_ops);
    ASSERT_TRUE(batch_ret.ok()) << batch_ret.result();

    // 2. Verify LITE3_PROT active_api_buffer_scope + zero-copy DATABASE_OP_EXECUTE_GENQUERY2_BUFFER
    irods::protocol::DataObjInp client_req;
    client_req.obj_path("/tempZone/home/rods/bench_file_0.bin");
    client_req.open_flags(O_RDONLY);
    client_req.cond_input().set("zone", "tempZone");

    auto wire_bbuf_res = client_req.encode_bbuf(LITE3_PROT, "rods6.0.0");
    ASSERT_TRUE(wire_bbuf_res.has_value());
    bytesBuf_t* wire_bbuf = *wire_bbuf_res;

    {
        irods::protocol::active_api_buffer_scope scope{wire_bbuf, LITE3_PROT};
        ASSERT_TRUE(irods::protocol::active_api_buffer_scope::has_active_request_buffer());

        auto server_req = irods::protocol::active_api_buffer_scope::try_get_active_request<irods::protocol::DataObjInp>();
        ASSERT_TRUE(server_req.has_value());
        EXPECT_EQ(server_req->obj_path(), "/tempZone/home/rods/bench_file_0.bin");
        EXPECT_EQ(server_req->cond_input().get("zone"), "tempZone");

        using gq2::builder::col;
        const auto q = gq2::builder::select({"DATA_ID", "DATA_NAME", "DATA_SIZE", "DATA_OWNER_NAME", "DATA_OWNER_ZONE"})
                           .where(col("DATA_NAME") == "bench_file_0.bin")
                           .build();
        const gq2::statement stmt = q;
        gq2::options opts{};

        lite3cpp::Buffer rows_buf;
        auto q_ret = plugin()->call<const gq2::statement*, const gq2::options*, lite3cpp::Buffer*>(
            nullptr, irods::DATABASE_OP_EXECUTE_GENQUERY2_BUFFER, nullptr, &stmt, &opts, &rows_buf);
        ASSERT_TRUE(q_ret.ok()) << q_ret.result();

        const gq2::rows_buffer_view view{rows_buf};
        ASSERT_FALSE(view.empty());
        EXPECT_EQ(view.cell_str(0, 1), "bench_file_0.bin");
        EXPECT_EQ(view.cell_str(0, 2), "65536");

        irods::protocol::RodsObjStat stat_reply;
        stat_reply.data_id(view.cell_str(0, 0));
        stat_reply.obj_size(65536);
        stat_reply.obj_type(DATA_OBJ_T);
        stat_reply.owner_name(view.cell_str(0, 3));
        stat_reply.owner_zone(view.cell_str(0, 4));
        irods::protocol::active_api_buffer_scope::set_active_reply(std::move(stat_reply));

        auto reply_buf = irods::protocol::active_api_buffer_scope::take_active_reply_buffer();
        ASSERT_TRUE(reply_buf.has_value());
        irods::protocol::RodsObjStat client_stat{std::move(*reply_buf)};
        EXPECT_EQ(client_stat.obj_size(), 65536);
        EXPECT_EQ(client_stat.owner_name(), "rods");
    }
    freeBBuf(wire_bbuf);

    // 3. Benchmark: Zero-Copy DATABASE_OP_EXECUTE_GENQUERY2_BUFFER vs JSON DATABASE_OP_EXECUTE_GENQUERY2
    using gq2::builder::col;
    const auto bench_q = gq2::builder::select({"DATA_ID", "DATA_NAME", "DATA_SIZE", "DATA_OWNER_NAME", "DATA_OWNER_ZONE"})
                             .where(col("DATA_OWNER_NAME") == "rods")
                             .build();
    const gq2::statement bench_stmt = bench_q;
    gq2::options bench_opts{};
    const std::vector<int> col_ids = {COL_D_DATA_ID, COL_DATA_NAME, COL_DATA_SIZE, COL_D_OWNER_NAME, COL_D_OWNER_ZONE};

    constexpr int kIterations = 100;

    const auto t0 = std::chrono::steady_clock::now();
    for (int iter = 0; iter < kIterations; ++iter) {
        lite3cpp::Buffer rows_buf;
        auto r = plugin()->call<const gq2::statement*, const gq2::options*, lite3cpp::Buffer*>(
            nullptr, irods::DATABASE_OP_EXECUTE_GENQUERY2_BUFFER, nullptr, &bench_stmt, &bench_opts, &rows_buf);
        ASSERT_TRUE(r.ok());
        genQueryOut_t* gq_out = nullptr;
        ASSERT_EQ(gq2::rows_buffer_to_genquery_out(rows_buf, col_ids, &gq_out), 0);
        ASSERT_NE(gq_out, nullptr);
        EXPECT_GE(gq_out->rowCnt, 10);
        freeGenQueryOut(&gq_out);
    }
    const auto t1 = std::chrono::steady_clock::now();

    const auto t2 = std::chrono::steady_clock::now();
    for (int iter = 0; iter < kIterations; ++iter) {
        char* json_out = nullptr;
        auto r = plugin()->call<const gq2::statement*, const gq2::options*, char**>(
            nullptr, irods::DATABASE_OP_EXECUTE_GENQUERY2, nullptr, &bench_stmt, &bench_opts, &json_out);
        ASSERT_TRUE(r.ok());
        ASSERT_NE(json_out, nullptr);
        genQueryOut_t* gq_out = nullptr;
        ASSERT_EQ(gq2::rows_to_genquery_out(std::string{json_out}, col_ids, &gq_out), 0);
        std::free(json_out);
        ASSERT_NE(gq_out, nullptr);
        EXPECT_GE(gq_out->rowCnt, 10);
        freeGenQueryOut(&gq_out);
    }
    const auto t3 = std::chrono::steady_clock::now();

    const double buffer_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    const double json_ms = std::chrono::duration<double, std::milli>(t3 - t2).count();

    // 4. Pure CPU Deserialization / Materialization Benchmark (10,000 iterations over 10-row result set)
    lite3cpp::Buffer sample_buf;
    ASSERT_TRUE((plugin()->call<const gq2::statement*, const gq2::options*, lite3cpp::Buffer*>(
        nullptr, irods::DATABASE_OP_EXECUTE_GENQUERY2_BUFFER, nullptr, &bench_stmt, &bench_opts, &sample_buf).ok()));
    const std::string sample_json = gq2::rows_buffer_view{sample_buf}.to_json().dump();

    constexpr int kCpuIterations = 10000;
    const auto c0 = std::chrono::steady_clock::now();
    for (int iter = 0; iter < kCpuIterations; ++iter) {
        genQueryOut_t* gq_out = nullptr;
        ASSERT_EQ(gq2::rows_buffer_to_genquery_out(sample_buf, col_ids, &gq_out), 0);
        freeGenQueryOut(&gq_out);
    }
    const auto c1 = std::chrono::steady_clock::now();

    const auto c2 = std::chrono::steady_clock::now();
    for (int iter = 0; iter < kCpuIterations; ++iter) {
        const auto j = gq2::rows_buffer_view{sample_buf}.to_json();
        const std::string dumped = j.dump();
        genQueryOut_t* gq_out = nullptr;
        ASSERT_EQ(gq2::rows_to_genquery_out(dumped, col_ids, &gq_out), 0);
        freeGenQueryOut(&gq_out);
    }
    const auto c3 = std::chrono::steady_clock::now();

    const auto c4 = std::chrono::steady_clock::now();
    std::size_t total_chars = 0;
    for (int iter = 0; iter < kCpuIterations; ++iter) {
        const gq2::rows_buffer_view view{sample_buf};
        for (std::size_t r = 0; r < view.size(); ++r) {
            for (std::size_t c = 0; c < 5; ++c) {
                total_chars += view.cell_str(r, c).size();
            }
        }
    }
    const auto c5 = std::chrono::steady_clock::now();
    EXPECT_GT(total_chars, 0u);

    const double cpu_buf_ms = std::chrono::duration<double, std::milli>(c1 - c0).count();
    const double cpu_json_ms = std::chrono::duration<double, std::milli>(c3 - c2).count();
    const double cpu_view_ms = std::chrono::duration<double, std::milli>(c5 - c4).count();

    std::cout << "\n==================================================================\n"
              << " iRODS 6 + L3KVG Query Pipeline Benchmark (" << kIterations << " RPCs, 10 rows)\n"
              << "------------------------------------------------------------------\n"
              << " End-to-End ZMQ + Zero-Copy Buffer   : " << std::fixed << std::setprecision(2) << buffer_ms << " ms ("
              << (buffer_ms * 1000.0 / kIterations) << " us/query)\n"
              << " End-to-End ZMQ + Legacy JSON String : " << std::fixed << std::setprecision(2) << json_ms << " ms ("
              << (json_ms * 1000.0 / kIterations) << " us/query)\n"
              << "------------------------------------------------------------------\n"
              << " Result Materialization CPU (" << kCpuIterations << " iters, 10 rows x 5 cols)\n"
              << " Zero-Copy rows_buffer_view (in-place) : " << std::fixed << std::setprecision(2) << cpu_view_ms << " ms ("
              << (cpu_view_ms * 1000.0 / kCpuIterations) << " us/iter)\n"
              << " Zero-Copy rows_buffer_to_genquery_out : " << std::fixed << std::setprecision(2) << cpu_buf_ms << " ms ("
              << (cpu_buf_ms * 1000.0 / kCpuIterations) << " us/iter)\n"
              << " JSON dump + parse + rows_to_genquery  : " << std::fixed << std::setprecision(2) << cpu_json_ms << " ms ("
              << (cpu_json_ms * 1000.0 / kCpuIterations) << " us/iter)\n"
              << " GenQueryOut Materialization Speedup   : " << std::fixed << std::setprecision(2) << (cpu_json_ms / cpu_buf_ms) << "x\n"
              << " Zero-Copy rows_buffer_view Speedup    : " << std::fixed << std::setprecision(2) << (cpu_json_ms / cpu_view_ms) << "x\n"
              << "==================================================================\n";
}
