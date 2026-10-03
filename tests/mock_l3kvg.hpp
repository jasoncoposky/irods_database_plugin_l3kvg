#pragma once

#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <mutex>
#include <regex>
#include "irods/catalog/binary_key.hpp"
#include "buffer.hpp"
#include "L3KVG/MutationBatch.hpp"
#include "L3KVG/KeyBuilder.hpp"

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
                        
                        std::cerr << "[MockServer] Received " << msgs.size() << " frames" << std::endl;
                        for (size_t i = 0; i < msgs.size(); ++i) {
                            std::cerr << "  Frame " << i << ": size=" << msgs[i].size() << " content=[" << msgs[i].to_string() << "]" << std::endl;
                        }

                        if (msgs.size() < 3) continue;

                        std::string cmd = msgs[2].to_string();
                        std::string key = (msgs.size() > 3) ? msgs[3].to_string() : "";
                        
                        if (cmd == "P") {
                            if (msgs.size() < 5) continue;
                            std::string payload = msgs[4].to_string();
                            std::lock_guard<std::mutex> lock(mu_);
                            
                            if (key.starts_with("n:{") && key.size() >= 3 + 16) {
                                try {
                                    uint64_t id = std::stoull(key.substr(3, 16), nullptr, 16);
                                    nodes_[id].id = id; nodes_[id].payload = payload;
                                    std::cerr << "[MockServer] Stored Node [" << std::hex << id << "]" << std::endl;
                                } catch (...) {
                                    generic_store_[key] = payload;
                                }
                            } else if (key.starts_with("e:out:{") && key.size() >= 7 + 16) {
                                try {
                                    uint64_t src = std::stoull(key.substr(7, 16), nullptr, 16);
                                    size_t label_start = 7 + 16 + 2;
                                    size_t label_end = key.find(':', label_start);
                                    size_t dst_start = (label_end != std::string::npos) ? key.find(":{", label_end) : std::string::npos;
                                    if (label_end != std::string::npos && dst_start != std::string::npos && key.size() >= dst_start + 2 + 16) {
                                        std::string label = key.substr(label_start, label_end - label_start);
                                        uint64_t dst = std::stoull(key.substr(dst_start + 2, 16), nullptr, 16);
                                        nodes_[src].edges.push_back({label, dst});
                                        std::cerr << "[MockServer] Stored Edge [" << std::hex << src << "] --(" << label << ")--> [" << std::hex << dst << "]" << std::endl;
                                    }
                                } catch (...) {
                                    generic_store_[key] = payload;
                                }
                            } else {
                                // Generic key (e.g. index)
                                generic_store_[key] = payload;
                                std::cerr << "[MockServer] Stored Generic Key [" << key << "] value=[" << payload << "]" << std::endl;
                            }
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "D") {
                            if (msgs.size() < 4) continue;
                            std::string key = msgs[3].to_string();
                            std::lock_guard<std::mutex> lock(mu_);
                            
                            if (key.starts_with("n:{") && key.size() >= 3 + 16) {
                                try {
                                    uint64_t id = std::stoull(key.substr(3, 16), nullptr, 16);
                                    nodes_.erase(id);
                                    std::cerr << "[MockServer] Deleted Node [" << std::hex << id << "]" << std::endl;
                                } catch (...) {
                                    generic_store_.erase(key);
                                }
                            } else if (key.starts_with("e:out:{") && key.size() >= 7 + 16) {
                                try {
                                    uint64_t src = std::stoull(key.substr(7, 16), nullptr, 16);
                                    size_t label_start = 7 + 16 + 2;
                                    size_t label_end = key.find(':', label_start);
                                    size_t dst_start = (label_end != std::string::npos) ? key.find(":{", label_end) : std::string::npos;
                                    if (label_end != std::string::npos && dst_start != std::string::npos && key.size() >= dst_start + 2 + 16) {
                                        std::string label = key.substr(label_start, label_end - label_start);
                                        uint64_t dst = std::stoull(key.substr(dst_start + 2, 16), nullptr, 16);
                                        
                                        auto it = nodes_.find(src);
                                        if (it != nodes_.end()) {
                                            auto& edges = it->second.edges;
                                            edges.erase(std::remove_if(edges.begin(), edges.end(), [&](const auto& e) {
                                                return e.first == label && e.second == dst;
                                            }), edges.end());
                                        }
                                        std::cerr << "[MockServer] Deleted Edge [" << std::hex << src << "] --(" << label << ")--> [" << std::hex << dst << "]" << std::endl;
                                    }
                                } catch (...) {
                                    generic_store_.erase(key);
                                }
                            } else {
                                generic_store_.erase(key);
                                std::cerr << "[MockServer] Deleted Generic Key [" << key << "]" << std::endl;
                            }

                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "B") {
                            if (msgs.size() < 4) continue;
                            const auto& payload_msg = msgs[3];
                            lite3cpp::Buffer buf(std::vector<uint8_t>(
                                static_cast<const uint8_t*>(payload_msg.data()),
                                static_cast<const uint8_t*>(payload_msg.data()) + payload_msg.size()
                            ));
                            size_t count = l3kvg::MutationBatch::item_count(buf);
                            std::lock_guard<std::mutex> lock(mu_);
                            for (size_t i = 0; i < count; ++i) {
                                auto item = l3kvg::MutationBatch::read_item(buf, i);
                                switch (item.op) {
                                    case l3kvg::MutationOp::PutNode: {
                                        nodes_[item.src].id = item.src;
                                        nodes_[item.src].payload = std::string(item.value);
                                        std::cerr << "[MockServer] Batch Stored Node [" << std::hex << item.src << "]" << std::endl;
                                        break;
                                    }
                                    case l3kvg::MutationOp::PutRaw: {
                                        generic_store_[std::string(item.key)] = std::string(item.value);
                                        std::cerr << "[MockServer] Batch Stored Generic Key [" << item.key << "] value=[" << item.value << "]" << std::endl;
                                        break;
                                    }
                                    case l3kvg::MutationOp::AddEdge: {
                                        nodes_[item.src].edges.push_back({std::string(item.label), item.dst});
                                        std::string in_key = std::string(l3kvg::KeyBuilder::edge_in_key(item.dst, item.label, item.src));
                                        generic_store_[in_key] = "{}";
                                        std::string out_key = std::string(l3kvg::KeyBuilder::edge_out_key(item.src, item.label, item.weight, item.dst));
                                        generic_store_[out_key] = "{}";
                                        std::cerr << "[MockServer] Batch Stored Edge [" << std::hex << item.src << "] --(" << item.label << ")--> [" << std::hex << item.dst << "]" << std::endl;
                                        break;
                                    }
                                    case l3kvg::MutationOp::DelNode: {
                                        nodes_.erase(item.src);
                                        break;
                                    }
                                    case l3kvg::MutationOp::DelRaw: {
                                        generic_store_.erase(std::string(item.key));
                                        break;
                                    }
                                    case l3kvg::MutationOp::DelEdge: {
                                        auto it = nodes_.find(item.src);
                                        if (it != nodes_.end()) {
                                            auto& edges = it->second.edges;
                                            edges.erase(std::remove_if(edges.begin(), edges.end(), [&](const auto& e) {
                                                return e.first == item.label && e.second == item.dst;
                                            }), edges.end());
                                        }
                                        break;
                                    }
                                }
                            }
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "G") {
                            if (msgs.size() < 4) continue;
                            std::string key = msgs[3].to_string();
                            std::string payload = "";
                            {
                                std::lock_guard<std::mutex> lock(mu_);
                                uint64_t id = 0;
                                bool is_node = false;
                                if (key.starts_with("n:{") && key.size() >= 19) {
                                    try { id = std::stoull(key.substr(3, 16), nullptr, 16); is_node = true; } catch(...) {}
                                } else if (key.size() == 16) {
                                    try { id = std::stoull(key, nullptr, 16); is_node = true; } catch(...) {}
                                }

                                if (is_node) {
                                    auto it = nodes_.find(id);
                                    if (it != nodes_.end()) payload = it->second.payload;
                                }
                                
                                if (payload.empty()) {
                                    auto it = generic_store_.find(key);
                                    if (it != generic_store_.end()) payload = it->second;
                                }
                                
                                if (payload.empty()) std::cerr << "[MockServer] GET Key [" << key << "] NOT FOUND" << std::endl;
                            }
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(payload.data(), payload.size()), zmq::send_flags::none);
                            std::cerr << "[MockServer] Sent Payload for [" << key << "]" << std::endl;
                            continue;
                        } else if (cmd == "N") {
                            if (msgs.size() < 5) continue;
                            uint64_t id = 0;
                            try { id = std::stoull(msgs[3].to_string(), nullptr, 16); } catch (...) {}
                            std::string label = msgs[4].to_string();
                            
                            std::vector<uint64_t> neighs;
                            {
                                std::lock_guard<std::mutex> lock(mu_);
                                auto it = nodes_.find(id);
                                if (it != nodes_.end()) {
                                    for (const auto& e : it->second.edges) {
                                        if (e.first == label) neighs.push_back(e.second);
                                    }
                                }
                            }
                            
                            nlohmann::json j = nlohmann::json::array();
                            for (auto n : neighs) {
                                char buf[17]; std::snprintf(buf, 17, "%016llx", (unsigned long long)n);
                                j.push_back(std::string(buf));
                            }
                            
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(j.dump()), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "I") {
                            if (msgs.size() < 5) continue;
                            uint64_t id = 0;
                            try { id = std::stoull(msgs[3].to_string(), nullptr, 16); } catch (...) {}
                            std::string label = msgs[4].to_string();
                            
                            std::vector<uint64_t> neighs;
                            {
                                std::lock_guard<std::mutex> lock(mu_);
                                for (const auto& [nid, node] : nodes_) {
                                    for (const auto& e : node.edges) {
                                        if (e.first == label && e.second == id) neighs.push_back(nid);
                                    }
                                }
                            }
                            
                            nlohmann::json j = nlohmann::json::array();
                            for (auto n : neighs) {
                                char buf[17]; std::snprintf(buf, 17, "%016llx", (unsigned long long)n);
                                j.push_back(std::string(buf));
                            }
                            
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(j.dump()), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "H") {
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(), zmq::send_flags::none);
                             continue;
                        } else if (cmd == "R") {
                             if (msgs.size() < 5) continue;
                             std::string query_json = msgs[4].to_string();
                             nlohmann::json q = nlohmann::json::parse(query_json);
                             
                             std::vector<uint64_t> sn;
                             try { sn = nlohmann::json::parse(msgs[3].to_string()).get<std::vector<uint64_t>>(); } catch (...) {}
                             std::unordered_set<uint64_t> sn_set(sn.begin(), sn.end());

                             nlohmann::json results = nlohmann::json::array();
                             std::string root_alias = q["root_alias"];
                             
                             std::lock_guard<std::mutex> lock(mu_);
                             for (const auto& [id, node] : nodes_) {
                                 if (!sn_set.empty() && !sn_set.count(id)) continue;
                                 bool match = true;
                                 std::function<bool(const nlohmann::json&)> eval_filters = [&](const nlohmann::json& filter_list) -> bool {
                                     for (const auto& f : filter_list) {
                                         if (f.contains("filters") && f["filters"].is_array()) {
                                             if (!eval_filters(f["filters"])) return false;
                                             continue;
                                         }
                                         if (!f.contains("alias") || f["alias"] != root_alias) continue;
                                         std::string key = f.value("key", "");
                                         std::string val = f.value("value", "");
                                         if (key == "_access_user") {
                                             std::string owner = node.get_attribute<std::string>("o");
                                             std::string n = node.get_attribute<std::string>("n");
                                             if (owner != val) {
                                                 bool is_public = (n == "/" || n.find('/', 1) == std::string::npos ||
                                                                   n.ends_with("/home") || n.ends_with("/trash") ||
                                                                   n.ends_with("/public"));
                                                 if (!is_public) return false;
                                             }
                                             continue;
                                         }
                                         int op = f.value("op", 0);
                                         std::string attr = node.get_attribute<std::string>(key);
                                         if (op == 0) {
                                             if (attr != val) return false;
                                         } else if (op == 1) {
                                             if (attr == val) return false;
                                         } else if (op == 6) {
                                             std::string regex_str = "^";
                                             for (char c : val) {
                                                 if (c == '%') regex_str += ".*";
                                                 else if (c == '_') regex_str += ".";
                                                 else if (c == '.' || c == '*' || c == '+' || c == '?' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '|') { regex_str += "\\"; regex_str += c; }
                                                 else regex_str += c;
                                             }
                                             regex_str += "$";
                                             try {
                                                 std::regex re(regex_str, std::regex_constants::icase);
                                                 if (!std::regex_match(attr, re)) return false;
                                             } catch (...) { return false; }
                                         }
                                     }
                                     return true;
                                 };

                                 if (q.contains("filters")) {
                                     match = eval_filters(q["filters"]);
                                 }
                                 
                                 if (match) {
                                     nlohmann::json row;
                                     if (q.contains("projections")) {
                                         for (const auto& p : q["projections"]) {
                                             if (p["alias"] == root_alias) {
                                                 std::string prop = p["property"];
                                                 std::string as = p["as"];
                                                 row[as] = node.get_attribute<std::string>(prop);
                                             }
                                         }
                                     }
                                     results.push_back(row);
                                 }
                             }
                             
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(results.dump()), zmq::send_flags::none);
                             continue;
                        } else if (cmd == "+") {
                             if (msgs.size() < 5) continue;
                             std::string key = msgs[3].to_string();
                             int64_t delta = 1;
                             try { delta = std::stoll(msgs[4].to_string()); } catch (...) {}
                             uint64_t val = 0;
                             {
                                 std::lock_guard<std::mutex> lock(mu_);
                                 auto it = sequences_.find(key);
                                 if (it == sequences_.end()) {
                                     sequences_[key] = 1000;
                                 }
                                 sequences_[key] += delta;
                                 val = sequences_[key];
                             }
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             std::string val_str = std::to_string(val);
                             socket_.send(zmq::message_t(val_str.data(), val_str.size()), zmq::send_flags::none);
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

        std::string get_generic_value(const std::string& key) const {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = generic_store_.find(key);
            return (it == generic_store_.end()) ? "" : it->second;
        }

    private:
        std::string endpoint_;
        zmq::context_t ctx_;
        zmq::socket_t socket_;
        std::atomic<bool> running_;
        std::thread server_thread_;
        mutable std::mutex mu_;
        std::unordered_map<uint64_t, MockNode> nodes_;
        std::unordered_map<std::string, std::string> generic_store_;
        std::unordered_map<std::string, uint64_t> sequences_;
    };

} // namespace irods::catalog::test
