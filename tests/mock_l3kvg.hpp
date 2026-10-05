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
#include <cstring>
#include <algorithm>
#include "irods/catalog/binary_key.hpp"
#include "buffer.hpp"
#include "json.hpp"
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
                lite3cpp::Buffer buf(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
                if constexpr (std::is_same_v<T, std::string>) {
                    auto t = buf.get_type(0, key);
                    if (t == lite3cpp::Type::String) {
                        return std::string(buf.get_str(0, key));
                    }
                    if (t == lite3cpp::Type::Int64) {
                        return std::to_string(buf.get_i64(0, key));
                    }
                    return "";
                } else if constexpr (std::is_integral_v<T>) {
                    auto t = buf.get_type(0, key);
                    if (t == lite3cpp::Type::Int64) {
                        return static_cast<T>(buf.get_i64(0, key));
                    }
                    if (t == lite3cpp::Type::String) {
                        try { return static_cast<T>(std::stoll(std::string(buf.get_str(0, key)))); } catch (...) {}
                    }
                    return T{};
                }
                return T{};
            }
        };

        MockL3KVGServer(const std::string& endpoint) 
            : endpoint_(endpoint), ctx_(1), socket_(ctx_, zmq::socket_type::router), running_(false) {}

        ~MockL3KVGServer() {
            stop();
        }

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

                        size_t data_idx = 1;
                        if (data_idx < msgs.size() && msgs[data_idx].size() == 0) {
                            data_idx++; // skip delimiter
                        }

                        uint32_t principal_id = 0;
                        if (data_idx < msgs.size() && msgs[data_idx].size() == 4) {
                            std::memcpy(&principal_id, msgs[data_idx].data(), 4);
                            data_idx++;
                        }
                        (void)principal_id;

                        if (data_idx >= msgs.size()) {
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                            continue;
                        }

                        std::string cmd = msgs[data_idx].to_string(); data_idx++;
                        
                        if (cmd == "P") {
                            if (msgs.size() < data_idx + 2) {
                                socket_.send(msgs[0], zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                continue;
                            }
                            std::string key = msgs[data_idx].to_string();
                            std::string payload = msgs[data_idx + 1].to_string();
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
                            if (msgs.size() < data_idx + 1) {
                                socket_.send(msgs[0], zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                continue;
                            }
                            std::string key = msgs[data_idx].to_string();
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
                            if (msgs.size() < data_idx + 1) {
                                socket_.send(msgs[0], zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                continue;
                            }
                            const auto& payload_msg = msgs[data_idx];
                            lite3cpp::Buffer buf(
                                static_cast<const uint8_t*>(payload_msg.data()),
                                payload_msg.size()
                            );
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
                            if (msgs.size() < data_idx + 1) {
                                socket_.send(msgs[0], zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                continue;
                            }
                            std::string key = msgs[data_idx].to_string();
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
                            if (msgs.size() < data_idx + 2) {
                                socket_.send(msgs[0], zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                continue;
                            }
                            uint64_t id = 0;
                            try { id = std::stoull(msgs[data_idx].to_string(), nullptr, 16); } catch (...) {}
                            std::string label = msgs[data_idx + 1].to_string();
                            
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
                            
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(neighs.data(), neighs.size() * sizeof(uint64_t)), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "I") {
                            if (msgs.size() < data_idx + 2) {
                                socket_.send(msgs[0], zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                continue;
                            }
                            uint64_t id = 0;
                            try { id = std::stoull(msgs[data_idx].to_string(), nullptr, 16); } catch (...) {}
                            std::string label = msgs[data_idx + 1].to_string();
                            
                            std::vector<uint64_t> neighs;
                            {
                                std::lock_guard<std::mutex> lock(mu_);
                                for (const auto& [nid, node] : nodes_) {
                                    for (const auto& e : node.edges) {
                                        if (e.first == label && e.second == id) neighs.push_back(nid);
                                    }
                                }
                            }
                            
                            socket_.send(msgs[0], zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                            socket_.send(zmq::message_t(neighs.data(), neighs.size() * sizeof(uint64_t)), zmq::send_flags::none);
                            continue;
                        } else if (cmd == "H") {
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                             continue;
                        } else if (cmd == "R") {
                             if (msgs.size() < data_idx + 2) {
                                 socket_.send(msgs[0], zmq::send_flags::sndmore);
                                 socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                 socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                 continue;
                             }
                             lite3cpp::Buffer q;
                             const auto& q_msg = msgs[data_idx + 1];
                             const uint8_t* q_data = static_cast<const uint8_t*>(q_msg.data());
                             if (q_msg.size() >= sizeof(lite3cpp::PackedNodeLayout) && (q_data[0] == 0x06 || q_data[0] == 0x07)) {
                                 q = lite3cpp::Buffer(q_data, q_msg.size());
                             } else {
                                 std::string raw = q_msg.to_string();
                                 q = lite3cpp::lite3_json::from_json_string(raw.empty() ? "{}" : raw);
                             }

                             std::vector<uint64_t> sn;
                             const auto& sn_msg = msgs[data_idx];
                             if (sn_msg.size() >= 2 && static_cast<const char*>(sn_msg.data())[0] == '[' && static_cast<const char*>(sn_msg.data())[sn_msg.size() - 1] == ']') {
                                 try {
                                     lite3cpp::Buffer sbuf = lite3cpp::lite3_json::from_json_string(sn_msg.to_string());
                                     if (sbuf.size() >= sizeof(lite3cpp::PackedNodeLayout)) {
                                         lite3cpp::NodeView nv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(sbuf.data()));
                                         if (nv.type() == lite3cpp::Type::Array) {
                                             for (uint32_t i = 0; i < nv.size(); ++i) {
                                                 if (sbuf.arr_get_type(0, i) == lite3cpp::Type::Int64) {
                                                     sn.push_back(static_cast<uint64_t>(sbuf.arr_get_i64(0, i)));
                                                 }
                                             }
                                         }
                                     }
                                 } catch (...) {}
                             } else if (sn_msg.size() % sizeof(uint64_t) == 0 && sn_msg.size() > 0) {
                                 sn.resize(sn_msg.size() / sizeof(uint64_t));
                                 std::memcpy(sn.data(), sn_msg.data(), sn_msg.size());
                             }
                             std::unordered_set<uint64_t> sn_set(sn.begin(), sn.end());

                             lite3cpp::Buffer res_buf;
                             res_buf.init_array();
                             std::string root_alias = (q.size() >= sizeof(lite3cpp::PackedNodeLayout) && q.get_type(0, "root_alias") == lite3cpp::Type::String) 
                                                      ? std::string(q.get_str(0, "root_alias")) : "";

                             std::lock_guard<std::mutex> lock(mu_);
                             for (const auto& [id, node] : nodes_) {
                                 if (!sn_set.empty() && !sn_set.count(id)) continue;
                                 bool match = true;
                                 std::function<bool(size_t, std::string_view)> eval_filter_group = [&](size_t filter_arr_ofs, std::string_view group_type) -> bool {
                                     lite3cpp::NodeView arr_nv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(q.data() + filter_arr_ofs));
                                     bool is_or = (group_type == "or");
                                     if (arr_nv.size() == 0) return !is_or;
                                     for (uint32_t i = 0; i < arr_nv.size(); ++i) {
                                         if (q.arr_get_type(filter_arr_ofs, i) != lite3cpp::Type::Object) continue;
                                         size_t f_ofs = q.arr_get_obj(filter_arr_ofs, i);
                                         bool item_match = true;
                                         if (q.get_type(f_ofs, "filters") == lite3cpp::Type::Array) {
                                             std::string sub_grp = (q.get_type(f_ofs, "group") == lite3cpp::Type::String) 
                                                 ? std::string(q.get_str(f_ofs, "group")) : "and";
                                             item_match = eval_filter_group(q.get_arr(f_ofs, "filters"), sub_grp);
                                         } else {
                                             if (q.get_type(f_ofs, "alias") != lite3cpp::Type::String || q.get_str(f_ofs, "alias") != root_alias) {
                                                 item_match = true;
                                             } else {
                                                 std::string key = (q.get_type(f_ofs, "key") == lite3cpp::Type::String) ? std::string(q.get_str(f_ofs, "key")) : "";
                                                 std::string val = (q.get_type(f_ofs, "value") == lite3cpp::Type::String) ? std::string(q.get_str(f_ofs, "value")) : "";
                                                 if (key == "_access_user") {
                                                     std::string owner = node.get_attribute<std::string>("o");
                                                     std::string n = node.get_attribute<std::string>("n");
                                                     if (owner != val) {
                                                         bool is_public = (n == "/" || n.find('/', 1) == std::string::npos ||
                                                                           n.ends_with("/home") || n.ends_with("/trash") ||
                                                                           n.ends_with("/public"));
                                                         if (!is_public) item_match = false;
                                                     }
                                                 } else {
                                                     int64_t op = (q.get_type(f_ofs, "op") == lite3cpp::Type::Int64) ? q.get_i64(f_ofs, "op") : 0;
                                                     std::string attr = node.get_attribute<std::string>(key);
                                                     if (op == 0) {
                                                         item_match = (attr == val);
                                                     } else if (op == 1) {
                                                         item_match = (attr != val);
                                                     } else if (op >= 2 && op <= 5) {
                                                         try {
                                                             int64_t a_num = std::stoll(attr);
                                                             int64_t v_num = std::stoll(val);
                                                             if (op == 2) item_match = (a_num > v_num);
                                                             else if (op == 3) item_match = (a_num >= v_num);
                                                             else if (op == 4) item_match = (a_num < v_num);
                                                             else if (op == 5) item_match = (a_num <= v_num);
                                                         } catch (...) {
                                                             if (op == 2) item_match = (attr > val);
                                                             else if (op == 3) item_match = (attr >= val);
                                                             else if (op == 4) item_match = (attr < val);
                                                             else if (op == 5) item_match = (attr <= val);
                                                         }
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
                                                             item_match = std::regex_match(attr, re);
                                                         } catch (...) { item_match = false; }
                                                     }
                                                 }
                                             }
                                         }
                                         if (is_or) {
                                             if (item_match) return true;
                                         } else {
                                             if (!item_match) return false;
                                         }
                                     }
                                     return !is_or;
                                 };

                                 if (q.size() >= sizeof(lite3cpp::PackedNodeLayout) && q.get_type(0, "filters") == lite3cpp::Type::Array) {
                                     match = eval_filter_group(q.get_arr(0, "filters"), "and");
                                 }

                                 if (match) {
                                     size_t row_ofs = res_buf.arr_append_obj(0);
                                     size_t fields_ofs = res_buf.set_obj(row_ofs, "fields");
                                     if (q.size() >= sizeof(lite3cpp::PackedNodeLayout) && q.get_type(0, "projections") == lite3cpp::Type::Array) {
                                         size_t projs_arr_ofs = q.get_arr(0, "projections");
                                         lite3cpp::NodeView p_nv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(q.data() + projs_arr_ofs));
                                         for (uint32_t pi = 0; pi < p_nv.size(); ++pi) {
                                             if (q.arr_get_type(projs_arr_ofs, pi) != lite3cpp::Type::Object) continue;
                                             size_t p_ofs = q.arr_get_obj(projs_arr_ofs, pi);
                                             if (q.get_type(p_ofs, "alias") == lite3cpp::Type::String && q.get_str(p_ofs, "alias") == root_alias) {
                                                 std::string prop = (q.get_type(p_ofs, "property") == lite3cpp::Type::String) ? std::string(q.get_str(p_ofs, "property")) : "";
                                                 std::string as = (q.get_type(p_ofs, "as") == lite3cpp::Type::String) ? std::string(q.get_str(p_ofs, "as")) : "";
                                                 res_buf.set_str(fields_ofs, as, node.get_attribute<std::string>(prop));
                                             }
                                         }
                                     }
                                 }
                             }

                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(res_buf.data(), res_buf.size()), zmq::send_flags::none);
                             continue;
                        } else if (cmd == "+") {
                             if (msgs.size() < data_idx + 2) {
                                 socket_.send(msgs[0], zmq::send_flags::sndmore);
                                 socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                 socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                 continue;
                             }
                             std::string key = msgs[data_idx].to_string();
                             int64_t delta = 1;
                             try { delta = std::stoll(msgs[data_idx + 1].to_string()); } catch (...) {}
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
                        } else if (cmd == "K") {
                             if (msgs.size() < data_idx + 1) {
                                 socket_.send(msgs[0], zmq::send_flags::sndmore);
                                 socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                                 socket_.send(zmq::message_t("ERR_MALFORMED", 13), zmq::send_flags::none);
                                 continue;
                             }
                             std::string prefix = msgs[data_idx].to_string();
                             lite3cpp::Buffer kbuf;
                             kbuf.init_array();
                             {
                                 std::lock_guard<std::mutex> lock(mu_);
                                 for (const auto& [k, v] : generic_store_) {
                                     if (k.starts_with(prefix)) {
                                         size_t e = kbuf.arr_append_obj(0);
                                         kbuf.set_str(e, "k", k);
                                         kbuf.set_str(e, "v", v);
                                     }
                                 }
                             }
                             socket_.send(msgs[0], zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(0), zmq::send_flags::sndmore);
                             socket_.send(zmq::message_t(kbuf.data(), kbuf.size()), zmq::send_flags::none);
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
        
        MockNode get_node(uint64_t id) const {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = nodes_.find(id);
            if (it != nodes_.end()) return it->second;
            return MockNode{};
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
