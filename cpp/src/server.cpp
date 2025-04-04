#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <fstream>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <nlohmann/json.hpp>

#include "proto/mini2.grpc.pb.h"
#include "proto/mini2.pb.h"
#include "parser/CSV.h"

using json = nlohmann::json;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using mini2::CollisionBatch;
using mini2::CollisionData;
using mini2::DatasetInfo;
using mini2::Empty;
using mini2::EntryPointService;
using mini2::InterServerService;
using mini2::RiskAssessment;

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

static bool g_simulate_shm_failure = false;
static bool g_ignore_grpc = true;

class SysVSharedMemoryManager
{
public:
    SysVSharedMemoryManager(key_t key, bool create)
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

    ~SysVSharedMemoryManager()
    {
        shmdt(buffer_);
        // shmctl(shmid_, IPC_RMID, NULL)
    }

    // Write a message (string) to the circular buffer.
    bool writeMessage(const std::string &msg)
    {
        // Force a simulated failure if the flag is set.
        if (g_simulate_shm_failure)
        {
            std::cerr << "Simulated shared memory failure." << std::endl;
            return false;
        }
        if (msg.size() > MAX_MESSAGE_SIZE)
        {
            std::cerr << "Message too large." << std::endl;
            return false;
        }
        int write = buffer_->header.write_index;
        int read = buffer_->header.read_index;
        if (((write + 1) % buffer_->header.capacity) == read)
        {
            std::cerr << "Shared memory buffer full." << std::endl;
            return false;
        }
        SharedMemorySlot &slot = buffer_->slots[write];
        slot.msg_len = msg.size();
        std::memcpy(slot.data, msg.data(), msg.size());
        buffer_->header.write_index = (write + 1) % buffer_->header.capacity;
        return true;
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
    // Server configuration.
    std::string server_id;
    std::string server_address;
    int server_port;

    // Overlay network configuration.
    std::map<std::string, ProcessNode> network_nodes;
    std::vector<std::string> connections;

    // For each local connection, store a unique SysVSharedMemoryManager.
    std::map<std::string, std::unique_ptr<SysVSharedMemoryManager>> shmManagers;
    std::atomic<bool> running_{true};
    std::vector<std::thread> readerThreads;

    // gRPC stubs for connections to other servers.
    std::map<std::string, std::unique_ptr<InterServerService::Stub>> server_stubs;

    // Data distribution counters.
    int total_records_seen = 0;
    int records_kept_locally = 0;
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
        if (shmManagers.find(connection_id) == shmManagers.end())
        {
            std::cerr << "No shared memory manager for " << connection_id << std::endl;
            return false;
        }
        std::string serialized_data;
        data.SerializeToString(&serialized_data);
        return shmManagers[connection_id]->writeMessage(serialized_data);
    }

    // Read a message from shared memory and deserialize into CollisionData.
    bool readFromSharedMemory(const std::string &connection_id, CollisionData &data)
    {
        
        if (shmManagers.find(connection_id) == shmManagers.end())
        {
            std::cerr << "No shared memory manager for " << connection_id << std::endl;
            return false;
        }
        std::string msg;
        if (!shmManagers[connection_id]->readMessage(msg))
        {
            return false;
        }
        printf("Read message from shared memory: %s\n", msg.c_str());
        return data.ParseFromString(msg);
    }

    // Initialize a gRPC channel (stub) to another server.
    void initServerStub(const std::string &srv_id)
    {
        if (network_nodes.find(srv_id) != network_nodes.end())
        {
            std::string target_address = network_nodes[srv_id].address + ":" +
                                         std::to_string(network_nodes[srv_id].port);
            auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
            server_stubs[srv_id] = InterServerService::NewStub(channel);
            std::cout << "Created channel to server " << srv_id << " at " << target_address << std::endl;
        }
    }

    // Forward data to a connected server via gRPC.
    void forwardDataToServer(const std::string &srv_id, const CollisionBatch &batch)
    {
        if (server_stubs.find(srv_id) == server_stubs.end())
        {
            initServerStub(srv_id);
        }
        ClientContext context;
        Empty response;
        Status status = server_stubs[srv_id]->ForwardData(&context, batch, &response);
        if (!status.ok())
        {
            std::cerr << "Failed to forward data to " << srv_id << ": " << status.error_message() << std::endl;
        }
    }

    // Decide whether to keep data locally based on a consistent hash.
    bool shouldKeepLocally(const CollisionData &data)
    {
        total_node_count = network_nodes.size();
        std::string key = data.borough() + data.zip_code() +
                          data.on_street_name() + data.cross_street_name();
        size_t hash_value = std::hash<std::string>{}(key);
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
    std::string chooseTargetServer(const CollisionData &data)
    {
        if (shouldKeepLocally(data))
        {
            return ""; // Keep locally.
        }
        std::string key = data.borough() + data.zip_code() +
                          data.on_street_name() + data.cross_street_name();
        size_t hash_value = std::hash<std::string>{}(key);
        std::vector<std::string> ordered_nodes;
        for (const auto &node_pair : network_nodes)
        {
            ordered_nodes.push_back(node_pair.first);
        }
        std::sort(ordered_nodes.begin(), ordered_nodes.end());
        int target_node_index = hash_value % total_node_count;
        std::string target_node_id = ordered_nodes[target_node_index];
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

    // Report distribution statistics with additional metrics.
    void reportEnhancedDistributionStats()
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

    // Analyze network topology.
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
    // Constructor uses entry_point_flag in the initializer list.
    GenericServer(const std::string &config_path)
        : entry_point_flag(false)
    {
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
        // Initialize shared memory for each local connection.
        for (size_t i = 0; i < connections.size(); i++)
        {
            std::string conn_id = connections[i];
            bool is_local = (network_nodes[conn_id].address == network_nodes[server_id].address);
            if (!is_local)
            {
                std::cout << "Connection to " << conn_id << " is remote ("
                          << network_nodes[conn_id].address << " vs "
                          << network_nodes[server_id].address << "), using gRPC only." << std::endl;
                continue;
            }
            std::cout << "Connection to " << conn_id << " is local, using shared memory." << std::endl;
            key_t shm_key = base_shm_key + i;
            try
            {
                shmManagers[conn_id] = std::make_unique<SysVSharedMemoryManager>(shm_key, true);
                // Launch a reader thread that drains the shared memory.
                readerThreads.emplace_back([this, conn_id]()
                                           {
                    int drainCount = 0;
                    CollisionData collision;
                    std::cerr << "Reader thread started for shared memory " << conn_id << std::endl;
                    while (running_) {
                        if (readFromSharedMemory(conn_id, collision)) {
                            drainCount++;
                            if (drainCount % 100 == 0) {
                                std::cout << "Drained " << drainCount 
                                          << " messages from connection " << conn_id << std::endl;
                            }
                        } else {
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        }
                    } });
            }
            catch (...)
            {
                std::cerr << "Failed to initialize shared memory for " << conn_id << std::endl;
            }
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
            total_records_seen++;
            if (count % 100 == 0)
            {
                std::cout << "Received " << count << " records" << std::endl;
            }
            bool keep_locally = shouldKeepLocally(collision);
            if (keep_locally)
            {
                records_kept_locally++;
                std::cout << "Record kept locally on entry point server" << std::endl;
                continue;
            }
            std::string target_server = chooseTargetServer(collision);
            if (!target_server.empty())
            {
                routing_stats[target_server]++;
                records_forwarded[target_server]++;
                bool target_local = (network_nodes[target_server].address == network_nodes[server_id].address);
                if (target_local && shmManagers.find(target_server) != shmManagers.end() &&
                    writeToSharedMemory(target_server, collision))
                {
                    if (count % 500 == 0)
                    {
                        std::cout << "Data written to shared memory for " << target_server << std::endl;
                    }
                }
                else {
                    if (!g_ignore_grpc) {
                        CollisionBatch batch;
                        *batch.add_collisions() = collision;
                        forwardDataToServer(target_server, batch);
                        if (count % 500 == 0)
                        {
                            std::cout << "Data sent via gRPC to " << target_server << std::endl;
                        }
                    } else {
                        std::cerr << "GRPC fallback disabled, record dropped" << std::endl;
                    }
                }
            }
            else
            {
                records_kept_locally++;
                std::cout << "No route available, keeping record locally" << std::endl;
            }
            if (entry_count % 1000 == 0)
            {
                std::cout << "\n--- PERIODIC DATA DISTRIBUTION REPORT ---\n";
                reportEnhancedDistributionStats();
                std::cout << "--- END REPORT ---\n\n";
            }
        }
        std::cout << "Finished receiving " << count << " records" << std::endl;
        std::cout << "Routing statistics:" << std::endl;
        for (const auto &stat : routing_stats)
        {
            std::cout << "  Sent to " << stat.first << ": " << stat.second << " records" << std::endl;
        }
        reportEnhancedDistributionStats();
        return Status::OK;
    }

    // Handle forwarded data from other servers.
    Status ForwardData(ServerContext *context,
                       const CollisionBatch *batch,
                       Empty *response) override
    {
        int batch_size = batch->collisions_size();
        std::cout << "Received forwarded batch with " << batch_size << " records" << std::endl;
        total_records_seen += batch_size;
        for (int i = 0; i < batch_size; i++)
        {
            const CollisionData &collision = batch->collisions(i);
            if (shouldKeepLocally(collision))
            {
                records_kept_locally++;
                std::cout << "Record kept locally based on content hash" << std::endl;
                continue;
            }
            std::string target_server = chooseTargetServer(collision);
            if (!target_server.empty())
            {
                records_forwarded[target_server]++;
                if (writeToSharedMemory(target_server, collision))
                {
                    std::cout << "Data written to shared memory for " << target_server << std::endl;
                }
                else
                {
                    if (writeToSharedMemory(target_server, collision))
                    {
                        std::cout << "Data written to shared memory for " << target_server << std::endl;
                    }
                    else {
                        if (!g_ignore_grpc) {
                            CollisionBatch new_batch;
                            *new_batch.add_collisions() = collision;
                            forwardDataToServer(target_server, new_batch);
                        } else {
                            std::cerr << "GRPC fallback disabled, record dropped" << std::endl;
                        }
                    }
                }
            }
        }
        reportEnhancedDistributionStats();
        return Status::OK;
    }

    // Handle sharing of analysis results.
    Status ShareAnalysis(ServerContext *context,
                         const RiskAssessment *assessment,
                         Empty *response) override
    {
        std::cout << "Received risk assessment for " << assessment->borough()
                  << " " << assessment->zip_code() << std::endl;
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

    // RPC to set dataset info.
    Status SetDatasetInfo(ServerContext *context,
                          const DatasetInfo *info,
                          Empty *response) override
    {
        total_dataset_size = info->total_size();
        std::cout << "Received dataset size information: " << total_dataset_size << " records" << std::endl;
        broadcastDatasetSize();
        return Status::OK;
    }

private:
    // Forward dataset size to all connected servers.
    void broadcastDatasetSize()
    {
        if (!entry_point_flag)
            return;
        DatasetInfo info;
        info.set_total_size(total_dataset_size);
        for (const std::string &srv_id : connections)
        {
            if (server_stubs.find(srv_id) == server_stubs.end())
            {
                initServerStub(srv_id);
            }
            ClientContext context;
            Empty response;
            Status status = server_stubs[srv_id]->SetTotalDatasetSize(&context, info, &response);
            if (status.ok())
            {
                std::cout << "Forwarded dataset size to server " << srv_id << std::endl;
            }
            else
            {
                std::cerr << "Failed to forward dataset size to " << srv_id << std::endl;
            }
        }
    }

    // RPC to set total dataset size.
    Status SetTotalDatasetSize(ServerContext *context,
                               const DatasetInfo *info,
                               Empty *response) override
    {
        total_dataset_size = info->total_size();
        std::cout << "Received total dataset size from another server: "
                  << total_dataset_size << " records" << std::endl;
        for (const std::string &srv_id : connections)
        {
            std::string peer = context->peer();
            if (peer.find(network_nodes[srv_id].address) != std::string::npos)
            {
                continue;
            }
            if (server_stubs.find(srv_id) == server_stubs.end())
            {
                initServerStub(srv_id);
            }
            ClientContext new_context;
            Empty new_response;
            Status status = server_stubs[srv_id]->SetTotalDatasetSize(&new_context, *info, &new_response);
            if (status.ok())
            {
                std::cout << "Forwarded dataset size to server " << srv_id << std::endl;
            }
            else
            {
                std::cerr << "Failed to forward dataset size to " << srv_id << std::endl;
            }
        }
        return Status::OK;
    }
};

// ---------------------------------------------------------------------------
// Main Function
// ---------------------------------------------------------------------------
int main(int argc, char **argv)
{
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