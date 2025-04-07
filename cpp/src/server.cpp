#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <atomic>
#include <mutex>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <nlohmann/json.hpp>
#include <csignal>
#include <chrono>
#include <thread>
#include <functional>

#include "proto/mini2.grpc.pb.h"
#include "proto/mini2.pb.h"
#include "../include/parser/CSV.h"
#include "../include/parser/SpatialAnalysis.h"

using json = nlohmann::json;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using mini2::CollisionData;
using mini2::Empty;
using mini2::EntryPointService;
using mini2::InterServerService;

constexpr int MAX_MESSAGE_SIZE = 1024; // Maximum size (in bytes) per message.
constexpr int NUM_SLOTS = 100;         // Number of fixed slots in the buffer.

struct SharedMemoryHeader
{
    int read_index;  // Next slot to read.
    int write_index; // Next slot to write.
    int capacity;    // Total number of slots.
};

struct SharedMemorySlot
{
    uint32_t msg_len;            // Actual length of the message.
    char data[MAX_MESSAGE_SIZE]; // Buffer for the message.
};

struct SharedMemoryBuffer
{
    SharedMemoryHeader header;
    SharedMemorySlot slots[NUM_SLOTS];
};

std::vector<mini2::CollisionData> collision_refs;

// Booleans for testing failure of shared memory and grpc
static bool g_simulate_shm_failure = false;
static bool g_simulate_grpc_failure = false;

class SharedMemoryManager
{
public:
    SharedMemoryManager(key_t key, bool create) : created_(create)
    {
        int flags = 0666;
        if (create)
        {
            flags |= IPC_CREAT;
        }
        shmid_ = shmget(key, sizeof(SharedMemoryBuffer), flags);
        if (shmid_ < 0)
        {
            std::cerr << "Failed to get shared memory segment for key " << key << std::endl;
            std::exit(1);
        }
        buffer_ = reinterpret_cast<SharedMemoryBuffer *>(shmat(shmid_, nullptr, 0));
        if (buffer_ == reinterpret_cast<void *>(-1))
        {
            std::cerr << "Failed to attach shared memory for key " << key << std::endl;
            std::exit(1);
        }
        if (create)
        {
            buffer_->header.read_index = 0;
            buffer_->header.write_index = 0;
            buffer_->header.capacity = NUM_SLOTS;
        }
    }

    ~SharedMemoryManager()
    {
        shmdt(buffer_);
        // Only remove the segment if this instance created it.
        if (created_)
        {
            shmctl(shmid_, IPC_RMID, NULL);
        }
    }

    // Write a message (string) to the circular buffer.
    bool writeMessage(const std::string &msg)
    {
        // Force a simulated failure if the flag is set.
        if (g_simulate_shm_failure)
        {
            return false;
        }
        if (msg.size() > MAX_MESSAGE_SIZE)
        {
            std::cerr << "Message too large." << std::endl;
            return false;
        }
        // Try multiple times if buffer is full
        const int MAX_ATTEMPTS = 5;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++)
        {
            int write = buffer_->header.write_index;
            int read = buffer_->header.read_index;

            if (((write + 1) % buffer_->header.capacity) == read)
            {
                // Buffer is full, wait a bit before retrying
                if (attempt < MAX_ATTEMPTS - 1)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    continue;
                }
                std::cerr << "Shared memory buffer full after " << MAX_ATTEMPTS << " attempts." << std::endl;
                return false;
            }
            SharedMemorySlot &slot = buffer_->slots[write];
            slot.msg_len = msg.size();
            std::memcpy(slot.data, msg.data(), msg.size());
            buffer_->header.write_index = (write + 1) % buffer_->header.capacity;
            return true;
        }
        return false; // Should not reach here.
    }

    // Read a message from the circular buffer.
    bool readMessage(std::string &msg)
    {
        int write = buffer_->header.write_index;
        int read = buffer_->header.read_index;
        if (read == write)
        {
            // Buffer is empty.
            return false;
        }
        SharedMemorySlot &slot = buffer_->slots[read];
        msg.assign(slot.data, slot.msg_len);
        buffer_->header.read_index = (read + 1) % buffer_->header.capacity;
        return true;
    }

private:
    int shmid_;
    SharedMemoryBuffer *buffer_;
    bool created_;
};

// ---------------------------------------------------------------------------
// Global Variables for Server
// ---------------------------------------------------------------------------
static int64_t g_total_client_records_sent = 0;   // Total records sent by clients.
static int64_t g_expected_total_dataset_size = 0; // Expected total number of CSV lines.

void setExpectedDatasetSize(int64_t size)
{
    g_expected_total_dataset_size = size;
}

// ---------------------------------------------------------------------------
// Structures for Network Configuration
// ---------------------------------------------------------------------------
struct ProcessNode
{
    std::string id;
    std::string address;
    int port;
    std::vector<std::string> connections;
};

// ---------------------------------------------------------------------------
// GenericServer Implementation (without processing)
// ---------------------------------------------------------------------------
class GenericServer : public EntryPointService::Service, public InterServerService::Service
{
private:
    std::mutex stub_mutex_;
    // Server configuration.
    std::string server_id;
    std::string server_address;
    int server_port;

    // Overlay network configuration.
    std::map<std::string, ProcessNode> network_nodes;
    std::vector<std::string> connections;

    // For each local connection, store a unique SharedMemoryManager.
    std::map<std::string, std::unique_ptr<SharedMemoryManager>> outgoingShmManagers;
    std::map<std::string, std::unique_ptr<SharedMemoryManager>> incomingShmManagers;
    std::atomic<bool> running_{true};
    std::vector<std::thread> readerThreads;

    // gRPC stubs for connections to other servers.
    std::map<std::string, std::shared_ptr<InterServerService::Stub>> server_stubs;

    // Data distribution counters.

    std::atomic<int> total_records_seen{0};
    std::atomic<int> records_kept_locally{0};
    std::mutex metricsMutex; // Protects records_forwarded updates.
    std::map<std::string, int> records_forwarded;

    // Total node count.
    int total_node_count;

    // Dataset size (expected number of CSV lines).
    int64_t total_dataset_size = 0;

    // Base key for shared memory.
    int base_shm_key = 1000;

    // Flag indicating if this server is the entry point.
    bool entry_point_flag;

    // Write a message (serialized CollisionData) to shared memory.
    bool writeToSharedMemory(const std::string &connection_id, const CollisionData &data)
    {
        if (outgoingShmManagers.find(connection_id) == outgoingShmManagers.end())
        {
            std::cerr << "No shared memory manager for " << connection_id << std::endl;
            return false;
        }
        std::string serialized_data;
        data.SerializeToString(&serialized_data);
        return outgoingShmManagers[connection_id]->writeMessage(serialized_data);
    }

    // Read a message from shared memory and deserialize into CollisionData. Flag for removal
    bool readFromSharedMemory(const std::string &connection_id, CollisionData &data)
    {
        if (incomingShmManagers.find(connection_id) == incomingShmManagers.end())
        {
            std::cerr << "No shared memory manager for " << connection_id << std::endl;
            return false;
        }
        std::string msg;
        if (!incomingShmManagers[connection_id]->readMessage(msg))
        {
            return false;
        }
        return data.ParseFromString(msg);
    }

    // Initialize a gRPC channel (stub) to another server.
    void initServerStub(const std::string &server_id)
    {
        std::string server_address = network_nodes[server_id].address + ":" +
                                     std::to_string(network_nodes[server_id].port);
        auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        server_stubs[server_id] = InterServerService::NewStub(channel);
    }

    // Forward data to a connected server via gRPC.
    void forwardDataToServer(const std::string &srv_id, const CollisionData &collision)
    {
        // Ensure the stub for the target server exists.
        if (server_stubs.find(srv_id) == server_stubs.end() || !server_stubs[srv_id])
        {
            std::cerr << "ERROR: GRPC stub for server " << srv_id << " is null. Attempting to reinitialize." << std::endl;
            initServerStub(srv_id);
            if (server_stubs.find(srv_id) == server_stubs.end() || !server_stubs[srv_id])
            {
                std::cerr << "ERROR: Failed to initialize GRPC stub for server " << srv_id << std::endl;
                return;
            }
        }
        // std::cout << "Received data from another server" << srv_id<<std::endl;
        ClientContext context;
        Empty response;
        Status status = server_stubs[srv_id]->ForwardData(&context, collision, &response);
        if (!status.ok())
        {
            std::cerr << "Failed to forward data to " << srv_id << ": " << status.error_message() << std::endl;
        }
    }

    size_t getHash(const CollisionData &data)
    {
        std::string key = data.borough() + data.crash_date() +
                          data.crash_time();
        size_t hash_value = std::hash<std::string>{}(key);
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> dis(0, 1000);
        hash_value += dis(gen);
        return hash_value;
    }

    // Decide whether to keep data locally based on a consistent hash.
    bool shouldKeepLocally(size_t hash_value)
    {
        total_node_count = network_nodes.size();
        int target_node_index = hash_value % total_node_count;
        std::vector<std::string> ordered_nodes;
        for (const auto &node_pair : network_nodes)
        {
            ordered_nodes.push_back(node_pair.first);
        }
        std::sort(ordered_nodes.begin(), ordered_nodes.end());

        for (size_t i = 0; i < ordered_nodes.size(); i++)
        {
            if (ordered_nodes[i] == server_id && i == target_node_index)
            {
                return true;
            }
        }
        return false;
    }

    // Choose target server for routing.
    std::string chooseTargetServer(size_t hash_value)
    {
        std::vector<std::string> ordered_nodes;
        for (const auto &node_pair : network_nodes)
        {
            ordered_nodes.push_back(node_pair.first);
        }
        std::sort(ordered_nodes.begin(), ordered_nodes.end());
        int target_node_index = hash_value % total_node_count;
        std::string target_node_id = ordered_nodes[target_node_index];

        // Check if our computed target is one of our children.
        for (const auto &conn : connections)
        {
            if (conn == target_node_id)
            {
                return conn;
            }
        }
        if (!connections.empty())
        {
            return connections[0];
        }
        return "";
    }

    // For parent nodes allocating shared memory
    void setupOutgoingSharedMemory()
    {
        // 'connections' are our children from our own configuration.
        for (size_t i = 0; i < connections.size(); i++)
        {
            std::string child_id = connections[i];
            // Only create the outgoing channel if the child is local.
            bool is_local = (network_nodes[child_id].address == network_nodes[server_id].address);
            if (!is_local)
            {
                std::cout << "Child " << child_id << " is remote; using gRPC only." << std::endl;
                continue;
            }
            // Unique key for each shared memory link, based on parent's id and child's id.
            key_t shm_key = base_shm_key + std::hash<std::string>{}(server_id + "_" + child_id) % 1000;
            try
            {
                // Create outgoing shared memory segment.
                outgoingShmManagers[child_id] = std::make_unique<SharedMemoryManager>(shm_key, true);
                std::cout << "Created outgoing shared memory for child " << child_id << " with key " << shm_key << std::endl;
            }
            catch (...)
            {
                std::cerr << "Failed to create outgoing shared memory for child " << child_id << std::endl;
            }
        }
    }

    // Attaches readers to child nodes and handles incoming data from shared memory
    // Also handles forwarding of data to child nodes if they exist via GRPC or shared memory
    // Number of reader threads in the system is equal to the number of parent nodes
    void readParentData()
    {
        // Identify our parent(s): any node whose connections list contains our server_id.
        std::vector<std::string> parents;
        for (const auto &pair : network_nodes)
        {
            const ProcessNode &node = pair.second;
            if (std::find(node.connections.begin(), node.connections.end(), server_id) != node.connections.end())
            {
                parents.push_back(node.id);
            }
        }
        // For each parent, attach to its outgoing shared memory segment.
        for (const std::string &parent_id : parents)
        {
            int key = base_shm_key + std::hash<std::string>{}(parent_id + "_" + server_id) % 1000;
            try
            {
                // Attach to parent's shared memory segment (as reader).
                incomingShmManagers[parent_id] = std::make_unique<SharedMemoryManager>(key, false);
                std::cout << "Child " << server_id << " attached to parent's (" << parent_id
                          << ") shared memory (key " << key << ")" << std::endl;
                // Spawns a reader thread to drain data from this parent's outgoing channel.
                readerThreads.emplace_back([this, parent_id]()
                                           {
                CollisionData collision;
                int consecutive_failures = 0;
                while (running_) {
                    std::string msg;
                    
                    if (incomingShmManagers[parent_id]->readMessage(msg))
                    {
                        consecutive_failures = 0;  // Reset failure counter on success
                        if (collision.ParseFromString(msg))
                        {
                            // For testing, we simply drain the message.
                            total_records_seen.fetch_add(1, std::memory_order_relaxed);
                            size_t hash_value = getHash(collision);
                            // If this node has no children, don't attempt forwarding.
                            if (!connections.empty() && !shouldKeepLocally(hash_value))
                            {
                                
                                std::string target_server = chooseTargetServer(hash_value);
                                if (!target_server.empty() && outgoingShmManagers.find(target_server) != outgoingShmManagers.end())
                                {
                                    if (writeToSharedMemory(target_server, collision))
                                    {
                                        // Increment forwarded counter.
                                        std::lock_guard<std::mutex> lock(metricsMutex);
                                        records_forwarded[target_server]++;
                                        // Commented out to reduce terminal bloat
                                        // std::cout << "Node " << server_id << " forwarded data to "
                                        //           << target_server << " via outgoing shared memory." << std::endl;
                                    }
                                    else
                                    {
                                        std::cout << "Node " << server_id << " failed to forward data to "
                                                  << target_server << std::endl;
                                    }
                                }
                                else
                                {
                                    // Fallback (e.g., via gRPC).
                                    if (!g_simulate_grpc_failure)
                                    {
                                        forwardDataToServer(target_server, collision);
                                        {
                                            std::lock_guard<std::mutex> lock(metricsMutex);
                                            records_forwarded[target_server]++;
                                        }
                                        std::cout << "Forwarded data via gRPC to " << target_server << std::endl;
                                    }
                                    else
                                    {
                                        records_kept_locally.fetch_add(1, std::memory_order_relaxed);
                                        // std::cout << "GRPC fallback disabled; record dropped" << std::endl;
                                    }
                                }
                            }
                            else
                            {
                                // keep data locally
                                std::cout<<collision.borough()<<"  "<<collision.zip_code()<<"  "<<collision.crash_date()<<"  "<<collision.crash_time()<<std::endl;
                                collision_refs.emplace_back(collision);
                                records_kept_locally.fetch_add(1, std::memory_order_relaxed);
                            }
                        }
                    }
                    else {
                        consecutive_failures++;
                        // If we've had too many consecutive failures, sleep longer
                        if (consecutive_failures > 100) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(50));
                        } else {
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        }
                    }
                } });
            }
            catch (...)
            {
                std::cerr << "Child " << server_id << " failed to attach to parent's (" << parent_id << ") shared memory" << std::endl;
            }
        }
    }

    // Report distribution statistics with additional metrics.
    void reportDistributionStats()
    {
        if (total_dataset_size == 0 && g_expected_total_dataset_size > 0)
        {
            total_dataset_size = g_expected_total_dataset_size;
        }
        std::cout << "\n--- DATA DISTRIBUTION REPORT ---\n";
        std::cout << "Server: " << server_id << std::endl;
        std::cout << "Total records seen: " << total_records_seen << std::endl;
        std::cout << "Records kept locally: " << records_kept_locally << std::endl;
        int forwarded = total_records_seen - records_kept_locally;
        std::cout << "Records forwarded: " << forwarded << std::endl;
        if (total_dataset_size > 0)
        {
            double localPercent = (records_kept_locally * 100.0) / total_dataset_size;
            double forwardedPercent = (forwarded * 100.0) / total_dataset_size;
            std::cout << "Local processing: " << localPercent << "% of total dataset" << std::endl;
            std::cout << "Forwarded processing: " << forwardedPercent << "% of total dataset" << std::endl;
        }
        std::cout << "--- END REPORT ---\n\n";
    }

    // Prints network topology.
    void analyzeNetworkTopology()
    {
        std::cout << "\n--- Network Topology for Server " << server_id << " ---\n";
        std::set<std::string> entry_points;
        std::set<std::string> intermediary_nodes;
        std::set<std::string> leaf_nodes;
        std::map<std::string, int> incoming_connections;
        for (const auto &node_pair : network_nodes)
        {
            incoming_connections[node_pair.first] = 0;
        }
        for (const auto &node_pair : network_nodes)
        {
            for (const auto &conn : node_pair.second.connections)
            {
                incoming_connections[conn]++;
            }
        }
        for (const auto &node_pair : network_nodes)
        {
            const std::string &node_id = node_pair.first;
            const auto &conns = node_pair.second.connections;
            entry_points.insert(node_id);
            if (conns.empty())
            {
                leaf_nodes.insert(node_id);
            }
            else
            {
                intermediary_nodes.insert(node_id);
            }
        }
        std::cout << "  Entry points: ";
        for (const auto &n : entry_points)
            std::cout << n << " ";
        std::cout << "\n  Intermediary nodes: ";
        for (const auto &n : intermediary_nodes)
            std::cout << n << " ";
        std::cout << "\n  Leaf nodes: ";
        for (const auto &n : leaf_nodes)
            std::cout << n << " ";
        std::cout << "\n  Connection map:\n";
        for (const auto &node_pair : network_nodes)
        {
            std::cout << "    " << node_pair.first << " -> ";
            if (node_pair.second.connections.empty())
            {
                std::cout << "(endpoint)";
            }
            else
            {
                for (const auto &conn : node_pair.second.connections)
                    std::cout << conn << " ";
            }
            std::cout << "(incoming: " << incoming_connections[node_pair.first] << ")\n";
        }
        std::cout << "--- End Network Topology ---\n\n";
    }

public:
    // Constructor uses entry_point_flag sourced from the config_X.jsons
    GenericServer(const std::string &config_path)
        : entry_point_flag(false)
    {
        collision_refs.reserve(2000000); // Reserve space for 2 million elements.

        if (g_expected_total_dataset_size > 0)
        {
            total_dataset_size = g_expected_total_dataset_size;
            std::cout << "Using dataset size from command line: "
                      << total_dataset_size << " records" << std::endl;
        }
        // Load configuration from JSON.
        std::ifstream config_file(config_path);
        if (!config_file.is_open())
        {
            std::cerr << "Failed to open config file: " << config_path << std::endl;
            exit(1);
        }
        json config;
        config_file >> config;
        server_id = config["server_id"];
        server_address = config["address"];
        server_port = config["port"];
        entry_point_flag = config["is_entry_point"];
        std::cout << "Configuring server " << server_id
                  << " at " << server_address << ":" << server_port << std::endl;
        // Parse network configuration.
        for (const auto &node : config["network"])
        {
            ProcessNode process_node;
            process_node.id = node["id"];
            process_node.address = node["address"];
            process_node.port = node["port"];
            for (const auto &conn : node["connections"])
            {
                process_node.connections.push_back(conn);
            }
            network_nodes[process_node.id] = process_node;
            if (process_node.id == server_id)
            {
                connections = process_node.connections;
            }
        }
        analyzeNetworkTopology();

        // Starts creating memory segments for nodes
        if (!connections.empty())
        {
            // This node has children—create outgoing shared memory segments.
            std::cout << "Node " << server_id << " has children. Creating outgoing channels for children." << std::endl;
            setupOutgoingSharedMemory();
        }

        if (!entry_point_flag)
        {
            // If not the entry point (root), attach to parent's outgoing channel.
            readParentData();
        }

        // Initialize gRPC stubs for all connections.
        for (const auto &conn : connections)
        {
            initServerStub(conn);
        }
        total_node_count = network_nodes.size();
        std::cout << "Network has " << total_node_count << " nodes" << std::endl;
    }

    ~GenericServer()
    {
        running_ = false;
        for (auto &t : readerThreads)
        {
            if (t.joinable())
            {
                t.join();
            }
        }
        reportDistributionStats();
    }

    // Handle incoming collision data from Python client (entry point).
    Status StreamCollisions(ServerContext *context,
                            grpc::ServerReader<CollisionData> *reader,
                            Empty *response) override
    {
        if (!entry_point_flag)
        {
            return Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "This server is not configured as an entry point");
        }
        CollisionData collision;
        int count = 0;
        std::map<std::string, int> routing_stats;
        static int entry_count = 0;
        while (reader->Read(&collision))
        {
            count++;
            entry_count++;
            total_records_seen.fetch_add(1, std::memory_order_relaxed);
            if (count % 100 == 0)
            {
                std::cout << "Received " << count << " records" << std::endl;
            }
            size_t hash_value = getHash(collision);
            if (shouldKeepLocally(hash_value))
            {
                collision_refs.emplace_back(collision);
                records_kept_locally.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            std::string target_server = chooseTargetServer(hash_value);
            routing_stats[target_server]++;
            {
                std::lock_guard<std::mutex> lock(metricsMutex);
                records_forwarded[target_server]++;
            }
            bool target_local = (network_nodes[target_server].address == network_nodes[server_id].address);
            if (target_local && outgoingShmManagers.find(target_server) != outgoingShmManagers.end() &&
                writeToSharedMemory(target_server, collision))
            {
                if (count % 500 == 0)
                {
                    std::cout << "Data written to shared memory for " << target_server << std::endl;
                }
            }
            else
            {
                if (!g_simulate_grpc_failure)
                {
                    forwardDataToServer(target_server, collision);
                    if (count % 500 == 0)
                    {
                        std::cout << "Data sent via gRPC to " << target_server << std::endl;
                    }
                }
                else
                {
                    // Here for testing. In prod should be removed
                    // std::cout << "GRPC failover off." << std::endl;
                    // std::cout << "Data to be sent to " << target_server << " Skipped" << std::endl;
                }
            }
            if (entry_count % 1000 == 0)
            {
                std::cout << "\n--- PERIODIC DATA DISTRIBUTION REPORT ---\n";
                reportDistributionStats();
                std::cout << "--- END REPORT ---\n\n";
            }
        }
        std::cout << "Finished receiving " << count << " records" << std::endl;
        std::cout << "Routing statistics:" << std::endl;
        for (const auto &stat : routing_stats)
        {
            std::cout << "  Sent to " << stat.first << ": " << stat.second << " records" << std::endl;
        }
        reportDistributionStats();
        return Status::OK;
    }

    // Handles forwarded data from other servers via GRPC
    Status ForwardData(ServerContext *context,
                       const CollisionData *collision,
                       Empty *response) override
    {
        std::cout << "Received data from another server" << std::endl;
        total_records_seen += 1;
        size_t hash_value = getHash(*collision);
        if (shouldKeepLocally(hash_value))
        {
            CollisionData collision_copy=*collision;
            std::cout<<collision_copy.borough()<<"  "<<collision_copy.zip_code()<<"  "<<collision_copy.crash_date()<<"  "<<collision_copy.crash_time()<<std::endl;
            collision_refs.emplace_back(collision_copy);
            records_kept_locally++;
            return Status::OK; // process collision data
        }
        if (connections.empty())
        {
            // This is a leaf node, so we should keep the data locally
            // instead of trying to forward it
            records_kept_locally++;
            return Status::OK;
        }
        std::string target_server = chooseTargetServer(hash_value);
        if (!target_server.empty())
        {
            records_forwarded[target_server]++;
            if (writeToSharedMemory(target_server, *collision))
            {
                std::cout << "Data written to shared memory for " << target_server << std::endl;
            }
            else
            {
                if (!g_simulate_grpc_failure)
                {
                    forwardDataToServer(target_server, *collision);
                }
                else
                {
                    std::cout << "Record Batch Skipped" << std::endl;
                }
            }
        }
        reportDistributionStats();
        return Status::OK;
    }

    // Get server address.
    std::string getServerAddress() const
    {
        return server_address + ":" + std::to_string(server_port);
    }

    // Check if this server is an entry point.
    bool isEntryPoint() const
    {
        return entry_point_flag;
    }
};

// ---------------------------------------------------------------------------
// Main Function
// ---------------------------------------------------------------------------
void signalHandler(int signum)
{
    std::cout << "\nInterrupt signal (" << signum << ") received. Cleaning up and exiting..." << std::endl;
    std::exit(signum); // std::exit() will run static destructors.
}

int main(int argc, char **argv)
{
    std::signal(SIGINT, signalHandler);
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <config_file.json> [expected_dataset_size]" << std::endl;
        return 1;
    }
    std::string config_path = argv[1];
    if (argc >= 3)
    {
        try
        {
            int64_t expected_size = std::stoll(argv[2]);
            g_expected_total_dataset_size = expected_size;
            std::cout << "Expected total dataset size: " << g_expected_total_dataset_size << " records" << std::endl;
        }
        catch (...)
        {
            std::cerr << "Invalid expected dataset size, ignoring" << std::endl;
        }
    }
    GenericServer server(config_path);
    ServerBuilder builder;
    builder.AddListeningPort(server.getServerAddress(), grpc::InsecureServerCredentials());
    if (server.isEntryPoint())
    {
        builder.RegisterService(static_cast<EntryPointService::Service *>(&server));
    }
    builder.RegisterService(static_cast<InterServerService::Service *>(&server));
    std::unique_ptr<Server> grpc_server(builder.BuildAndStart());
    std::cout << "Server " << (server.isEntryPoint() ? "(entry point) " : "")
              << "listening on " << server.getServerAddress() << std::endl;
    grpc_server->Wait();
    return 0;
}