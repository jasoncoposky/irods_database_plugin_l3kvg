#pragma once

#include <gtest/gtest.h>
#include <dlfcn.h>
#include "irods/irods_database_plugin.hpp"
#include "mock_l3kvg.hpp"

namespace irods::catalog::test {

    class PluginTestFixture : public ::testing::Test {
    protected:
        void SetUp() override {
            int port = 5570 + (rand() % 100);
            std::string endpoint = "tcp://127.0.0.1:" + std::to_string(port);
            mock_server_ = std::make_unique<MockL3KVGServer>(endpoint);
            mock_server_->start();
            endpoint_ = endpoint;

            // Load the plugin
            char cwd[1024];
            if (getcwd(cwd, sizeof(cwd))) {
                std::string plugin_path = std::string(cwd) + "/libirods_database_plugin_l3kvg.so";
                handle_ = dlopen(plugin_path.c_str(), RTLD_NOW);
            }
            if (!handle_) {
                throw std::runtime_error("Failed to load plugin: " + std::string(dlerror()));
            }

            typedef irods::database* (*factory_t)(const std::string&, const std::string&);
            auto factory = (factory_t)dlsym(handle_, "plugin_factory");
            if (!factory) {
                throw std::runtime_error("Failed to find plugin_factory");
            }

            plugin_ = factory("l3kvg_inst", "l3kvg_ctx");
        }

        void TearDown() override {
            if (plugin_) delete plugin_;
            if (handle_) dlclose(handle_);
            mock_server_->stop();
        }

        MockL3KVGServer* server() { return mock_server_.get(); }
        irods::database* plugin() { return plugin_; }
        std::string endpoint() const { return endpoint_; }

    private:
        void* handle_ = nullptr;
        irods::database* plugin_ = nullptr;
        std::unique_ptr<MockL3KVGServer> mock_server_;
        std::string endpoint_;
    };

} // namespace irods::catalog::test
