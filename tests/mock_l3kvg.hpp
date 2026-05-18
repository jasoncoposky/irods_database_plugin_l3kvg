#pragma once

#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <mutex>
#include <regex>
#include "irods/catalog/binary_key.hpp"
#include "buffer.hpp"

namespace irods::catalog::test {

    class MockL3KVGServer {
    public:
        struct MockNode {
            uint64_t id = 0;
            std::string payload;
            std::vector<std::pair<std::string, uint64_t>> edges;
        };

        MockL3KVGServer(const std::string& endpoint) 
            : endpoint_(endpoint), ctx_(1), socket_(ctx_, zmq::socket_type::router), running_(false) {}

        void start() {
            socket_.bind(endpoint_);
            running_ = true;
            server_thread_ = std::thread([this]() {
                while (running_) {
                    std::vector<zmq::message_t> msgs;
                    try {
                        auto res = zmq::recv_multipart(socket_, std::back_inserter(msgs), zmq::recv_flags::dontwait);
                        if (!res) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            continue;
                        }
                        
                        if (msgs.size() < 3) continue;

                        // [Identity] [Empty] [Cmd] [Args...]
                        std::string cmd = msgs[2].to_string();
                        
                        if (cmd == "P") {
                            if (msgs.size() < 5) continue;
                            std::string key = msgs[3].to_string();
                            std::string payload = msgs[4].to_string();
                            
                            std::lock_guard<std::mutex> lock(mu_);
                            
                            // Node: n:{id}
                            size_t n_start = key.find("n:{");
                            if (n_start != std::string::npos) {
                                uint64_t id = std::stoull(key.substr(n_start + 3, 16), nullptr, 16);
                                nodes_[id].id = id;
                                nodes_[id].payload = payload;
                            }
                            
                            // Edge: e:out:{src}:{label}:{weight}:{dst}
                            size_t e_start = key.find("e:out:{");
                            if (e_start != std::string::npos) {
                                uint64_t src = std::stoull(key.substr(e_start + 7, 16), nullptr, 16);
                                size_t label_start = e_start + 7 + 16 + 2;
                                size_t label_end = key.find(':', label_start);
                                std::string label = key.substr(label_start, label_end - label_start);
                                size_t dst_start = key.find(":{", label_end + 13); // skip weight
                                uint64_t dst = std::stoull(key.substr(dst_start + 2, 16), nullptr, 16);
                                nodes_[src].edges.push_back({label, dst});
                            }
                        } else if (cmd == "G") {
                            // ... return dummy payload ...
                        } else if (cmd == "R") {
                             // Query: return empty array for now
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t("[]", 2), zmq::send_flags::none);
                             continue;
                        }

                        // Send Ack
                        socket_.send(msgs[0], zmq::send_flags::sndmore);
                        socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                        socket_.send(zmq::message_t("OK", 2), zmq::send_flags::none);

                    } catch (...) { if (!running_) break; }
                }
            });
        }

        void stop() {
            if (running_) {
                running_ = false;
                if (server_thread_.joinable()) server_thread_.join();
                socket_.close();
                ctx_.close();
            }
        }

        bool has_node(uint64_t id) const {
            std::lock_guard<std::mutex> lock(mu_);
            return nodes_.find(id) != nodes_.end();
        }
        
        const MockNode& get_node(uint64_t id) const {
            std::lock_guard<std::mutex> lock(mu_);
            return nodes_.at(id);
        }

    private:
        std::string endpoint_;
        zmq::context_t ctx_;
        zmq::socket_t socket_;
        std::atomic<bool> running_;
        std::thread server_thread_;
        mutable std::mutex mu_;
        std::unordered_map<uint64_t, MockNode> nodes_;
    };

} // namespace irods::catalog::test
