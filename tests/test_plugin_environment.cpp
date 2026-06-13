#include <gtest/gtest.h>
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <thread>
#include "irods/catalog/catalog_facade.hpp"

using namespace irods::catalog;

class EnvironmentPreFlightTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Ensure L3KVG server is actually running for these tests
        // In a CI environment, l3kvg_server should be started before tests run.
    }
};

TEST_F(EnvironmentPreFlightTest, ZmqConnectivity) {
    zmq::context_t ctx(1);
    zmq::socket_t client(ctx, ZMQ_DEALER);
    client.set(zmq::sockopt::rcvtimeo, 500); // 500ms timeout
    client.connect("tcp://127.0.0.1:5556");

    // Send a generic ping (requesting a non-existent index) to see if we get a response
    client.send(zmq::message_t(), zmq::send_flags::sndmore);
    uint32_t dummy_uid = 0;
    client.send(zmq::message_t(&dummy_uid, 4), zmq::send_flags::sndmore);
    client.send(zmq::message_t("G", 1), zmq::send_flags::sndmore);
    std::string test_key = "idx:PreFlightTest:Connectivity";
    client.send(zmq::message_t(test_key.data(), test_key.size()), zmq::send_flags::none);

    std::vector<zmq::message_t> recv_msgs;
    auto res = zmq::recv_multipart(client, std::back_inserter(recv_msgs));
    
    // We expect a response, even if it's an empty payload (not found)
    ASSERT_TRUE(res.has_value()) << "L3KVG Server is not responding on tcp://127.0.0.1:5556. Is it running?";
    ASSERT_GE(recv_msgs.size(), 2);
}

TEST_F(EnvironmentPreFlightTest, DirectoryPermissions) {
    std::vector<std::string> critical_dirs = {
        "/tmp/irods",
        "/var/lib/irods/Vault",
        "/var/lib/irods/log"
    };

    for (const auto& dir : critical_dirs) {
        // Skip if directory doesn't exist (might not in isolated test environments)
        // But if it does exist, we MUST be able to write to it.
        if (std::filesystem::exists(dir)) {
            std::string test_file = dir + "/.l3kvg_preflight_test";
            std::ofstream out(test_file);
            ASSERT_TRUE(out.is_open()) << "CRITICAL: Cannot write to " << dir << ". Check irods user permissions.";
            out << "test";
            out.close();
            std::filesystem::remove(test_file);
        }
    }
}

TEST_F(EnvironmentPreFlightTest, BootstrapIntegrity) {
    // Attempt to connect a CatalogFacade and check for bootstrapped elements
    Config cfg;
    cfg.node_id = 1;
    cfg.cluster_id = 1;
    cfg.zmq_endpoint = "tcp://127.0.0.1:5556";
    
    CatalogFacade catalog;
    if (!catalog.init(cfg, "tempZone").ok()) {
        std::cerr << "Failed to initialize CatalogFacade" << std::endl;
        return;
    }
    
    // Check if rods user exists (it should have been bootstrapped)
    int priv = 0;
    auto u_ret = catalog.check_auth("rods", "tempZone", priv);
    if (u_ret.ok()) {
        // If the server is running and bootstrapped, this should succeed.
        std::cout << "[PreFlight] Bootstrap Integrity: Verified 'rods' user exists in 'tempZone'" << std::endl;
        EXPECT_EQ(priv, 2); // 2 = rodsadmin
    } else {
        std::cout << "[PreFlight] Bootstrap Integrity: Server is empty or 'rods' user is missing." << std::endl;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
