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

            template<typename T>
            T get_attribute(const std::string& key) const {
                if (payload.empty()) return T{};
                lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                if constexpr (std::is_same_v<T, std::string>) {
                    return std::string(buf.get_str(0, key));
                } else if constexpr (std::is_integral_v<T>) {
                    return static_cast<T>(buf.get_i64(0, key));
                }
                return T{};
            }
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
                            std::this_thread::sleep_for(std::chrono::milliseconds(5));
                            continue;
                        }
                        
                        if (msgs.size() < 4) continue;

                        std::string cmd = msgs[3].to_string();
                        std::string key = (msgs.size() > 4) ? msgs[4].to_string() : "";
                        
                        if (cmd == "P") {
                            if (msgs.size() < 6) continue;
                            std::string payload = msgs[5].to_string();
                            std::lock_guard<std::mutex> lock(mu_);
                            
                            size_t n_start = key.find("n:{");
                            if (n_start != std::string::npos) {
                                uint64_t id = std::stoull(key.substr(n_start + 3, 16), nullptr, 16);
                                nodes_[id].id = id; nodes_[id].payload = payload;
                                std::cerr << "[MockServer] Stored Node [" << std::hex << id << "]" << std::endl;
                            }
                            
                            size_t e_start = key.find("e:out:{");
                            if (e_start != std::string::npos) {
                                uint64_t src = std::stoull(key.substr(e_start + 7, 16), nullptr, 16);
                                size_t label_start = e_start + 7 + 16 + 2;
                                size_t label_end = key.find(':', label_start);
                                std::string label = key.substr(label_start, label_end - label_start);
                                size_t dst_start = key.find(":{", label_end + 13);
                                uint64_t dst = std::stoull(key.substr(dst_start + 2, 16), nullptr, 16);
                                nodes_[src].edges.push_back({label, dst});
                                std::cerr << "[MockServer] Stored Edge [" << std::hex << src << "] --(" << label << ")--> [" << std::hex << dst << "]" << std::endl;
                            }
                            // Fire-and-forget, no reply for P
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "D") {
                            if (msgs.size() < 5) continue;
                            std::string key = msgs[4].to_string();
                            
                            if (key.starts_with("n:{")) {
                                uint64_t id = std::stoull(key.substr(3, 16), nullptr, 16);
                                nodes_.erase(id);
                                std::cerr << "[MockServer] Deleted Node [" << std::hex << id << "]" << std::endl;
                            } else if (key.starts_with("e:out:{")) {
                                uint64_t src = std::stoull(key.substr(7, 16), nullptr, 16);
                                size_t label_start = 7 + 16 + 2;
                                size_t label_end = key.find(':', label_start);
                                std::string label = key.substr(label_start, label_end - label_start);
                                size_t dst_start = key.find(":{", label_end + 13);
                                uint64_t dst = std::stoull(key.substr(dst_start + 2, 16), nullptr, 16);
                                
                                auto& edges = nodes_[src].edges;
                                edges.erase(std::remove_if(edges.begin(), edges.end(), [&](const auto& e) {
                                    return e.first == label && e.second == dst;
                                }), edges.end());
                                std::cerr << "[MockServer] Deleted Edge [" << std::hex << src << "] --(" << label << ")--> [" << std::hex << dst << "]" << std::endl;
                            }

                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "G") {
                            if (msgs.size() < 5) continue;
                            uint64_t id = std::stoull(key, nullptr, 16);
                            std::string payload = "";
                            {
                                std::lock_guard<std::mutex> lock(mu_);
                                auto it = nodes_.find(id);
                                if (it != nodes_.end()) payload = it->second.payload;
                                else std::cerr << "[MockServer] GET Node [" << std::hex << id << "] NOT FOUND" << std::endl;
                            }
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(payload.data(), payload.size()), zmq::send_flags::none);
                            std::cerr << "[MockServer] Sent Payload for [" << std::hex << id << "]" << std::endl;
                            continue;
                        } else if (cmd == "H") {
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(), zmq::send_flags::none);
                             continue;
                        } else if (cmd == "R") {
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t("[]", 2), zmq::send_flags::none);
                             continue;
                        }

                        // Unknown command, send dummy ack to avoid hanging
                        socket_.send(msgs[0], zmq::send_flags::sndmore);
                        socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                        socket_.send(zmq::message_t("UNK", 3), zmq::send_flags::none);

                    } catch (const std::exception& e) {
                        std::cerr << "[MockServer] Error: " << e.what() << std::endl;
                    } catch (...) { break; }
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
