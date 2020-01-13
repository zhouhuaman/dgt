/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_VAN_H_
#define PS_INTERNAL_VAN_H_
#include <unordered_map>
#include <mutex>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <ctime>
#include <unordered_set>
#include "ps/base.h"
#include "ps/internal/message.h"
#include "customer.h"
#ifndef ADAPTIVE_K
#define ADAPTIVE_K
#endif
#ifndef ENCODE
#define ENCODE
#endif
namespace ps {
class Resender;

/**
 * \brief Van sends messages to remote nodes
 *
 * If environment variable PS_RESEND is set to be 1, then van will resend a
 * message if it no ACK messsage is received within PS_RESEND_TIMEOUT millisecond
 */
class Van {
 public:
    /**
     * \brief create Van
     * \param type zmq, socket, ...
     */
    static Van *Create(const std::string &type);

    /** \brief constructer, do nothing. use \ref Start for real start */
    Van() {}

    /**\brief deconstructer, do nothing. use \ref Stop for real stop */
    virtual ~Van() {}

    /**
     * \brief start van
     *
     * must call it before calling Send
     *
     * it initalizes all connections to other nodes.  start the receiving
     * threads, which keeps receiving messages. if it is a system
     * control message, give it to postoffice::manager, otherwise, give it to the
     * accoding app.
     */
    virtual void Start(int customer_id);

    /**
     * \brief send a message, It is thread-safe
     * \return the number of bytes sent. -1 if failed
     */
#ifdef DOUBLE_CHANNEL
#ifdef ADAPTIVE_K
    float Average_tp();
#endif
    /*channel = 0 means tcp channel 1->udp channel tag = 0 means send imediately.*/
	int Send(Message &msg, int channel = 0, int tag = 0);
    int Classifier( Message& msg, int channel=0, int tag=0);
    void MergeMsg(Message* msg1, Message* msg2);
    void ZeroMsg(Message* msg1);
    void ZeroSArray(char *sa,int size);
    void MergeSArray(char* sa1, char* sa2, int size);
    void ReduceSArray(char* sa,int size,int push_var);
    bool AsynAccept(Message* msg);
    
    void Important_scheduler();
    void Unimportant_scheduler();
    int Important_send(Message& msg);
    int Unimportant_send(Message& msg);
#ifdef CHANNEL_MLR
    void Update_Sendbuff( int timestamp);
#endif
#else
    int Send(Message &msg);
#endif
#ifdef ENCODE
    float encode(Message& msg);
    void decode(Message& msg);
    void msg_float_print(Message& msg, int n);
    std::unordered_map<int, SArray<char>> residual;
    int enable_encode=0;
#endif
    /**
     * \brief return my node
     */
    inline const Node &my_node() const {
      CHECK(ready_) << "call Start() first";
      return my_node_;
    }

    /**
     * \brief stop van
     * stop receiving threads
     */
    virtual void Stop();

    /**
     * \brief get next available timestamp. thread safe
     */
    inline int GetTimestamp() { return timestamp_++; }

    /**
     * \brief whether it is ready for sending. thread safe
     */
    inline bool IsReady() { return ready_; }

 protected:
    /**
     * \brief connect to a node
     */
    virtual void Connect(const Node &node) = 0;
#ifdef DOUBLE_CHANNEL
	virtual void Connect_UDP(const Node &node) = 0;
#endif
    /**
     * \brief bind to my node
     * do multiple retries on binding the port. since it's possible that
     * different nodes on the same machine picked the same port
     * \return return the port binded, -1 if failed.
     */
    virtual int Bind(const Node &node, int max_retry) = 0;
#ifdef DOUBLE_CHANNEL
	 virtual std::vector<int> Bind_UDP(const Node &node, int max_retry) = 0;
#endif


	virtual int RecvMsg_TCP(Message *msg) = 0;
	virtual int RecvMsg_UDP(int channel, Message *msg) = 0;

    /**
     * \brief block until received a message
     * \return the number of bytes received. -1 if failed or timeout
     */
    virtual int RecvMsg(Message *msg) = 0;

  

    virtual int SendMsg_UDP(int channel, Message &msg, int tag = 0) = 0;
	virtual int SendMsg_TCP(Message &msg, int tag = 0) = 0;

	 /**
     * \brief send a mesage
     * \return the number of bytes sent
     */
    virtual int SendMsg(Message &msg) = 0;

    /**
     * \brief pack meta into a string
     */
    void PackMeta(const Meta &meta, char **meta_buf, int *buf_size);

    /**
     * \brief unpack meta from a string
     */
    void UnpackMeta(const char *meta_buf, int buf_size, Meta *meta);

    Node scheduler_;
    Node my_node_;
    bool is_scheduler_;
    std::mutex start_mu_;
 public:
    /** msg resender */
    Resender *resender_ = nullptr;
 private:
    /** thread function for receving */
	/*if DOUBLE_CHANNEL, Receiving=> Receiving_TCP and  Receiving_UDP*/

	void Receiving_TCP();
	void Receiving_UDP(int channel);

    void Receiving();


    /** thread function for heartbeat */
    void Heartbeat();

    // node's address string (i.e. ip:port) -> node id
    // this map is updated when ip:port is received for the first time
    std::unordered_map<std::string, int> connected_nodes_;
    // maps the id of node which is added later to the id of node
    // which is with the same ip:port and added first
    std::unordered_map<int, int> shared_node_mapping_;

    /** whether it is ready for sending */
    std::atomic<bool> ready_{false};
    std::atomic<size_t> send_bytes_{0};
    size_t recv_bytes_ = 0;
    int num_servers_ = 0;
    int num_workers_ = 0;
#ifdef ADAPTIVE_K
    unsigned long monitor_count = 0;
#endif
#ifdef RECONSTRUCT
    std::unordered_map<int,std::unordered_map<int, std::unordered_map<int,Meta>>> meta_map;
    std::unordered_map<int,std::unordered_map<int, std::unordered_map<int,SArray<char>>>> recv_map;
    std::unordered_map<int,std::unordered_map<int, std::unordered_map<int,Message>>> msg_map;
    std::unordered_map<int,std::unordered_map<int, Message>> msg_buffer;
    std::unordered_map<int,std::unordered_map<int, std::unordered_map<int,int>>> recv_flag;
    int msg_size_limit = 4096;
    int reconstruct = 0;
    unsigned int ns_delay = 0;
    ThreadsafeQueue<Message> important_queue_;
    ThreadsafeQueue<Message> unimportant_queue_;
#endif
#ifdef DOUBLE_CHANNEL
	 /** the thread for receiving tcp messages */
    std::unique_ptr<std::thread> tcp_receiver_thread_;
	
	/** the thread for receiving udp messages */
   
    std::unique_ptr<std::thread> udp_receiver_thread_[8];
    std::vector<std::unique_ptr<std::thread>> udp_receiver_thread_vec;
    std::unique_ptr<std::thread> important_scheduler_thread_;
    std::unique_ptr<std::thread> unimportant_scheduler_thread_;
#else
    /** the thread for receiving messages */
    std::unique_ptr<std::thread> receiver_thread_;
#endif
    /** the thread for sending heartbeat */
    std::unique_ptr<std::thread> heartbeat_thread_;
    std::vector<int> barrier_count_;
    
    int drop_rate_ = 0;
    int tcp_recv = 0;
    int udp_recv = 0;
    std::atomic<int> timestamp_{0};
    int init_stage = 0;
#ifdef DOUBLE_CHANNEL
	std::mutex mu_;
    std::mutex merge_mu_;
    
#endif

    /**
     * \brief processing logic of AddNode message for scheduler
     */
    void ProcessAddNodeCommandAtScheduler(Message* msg, Meta* nodes, Meta* recovery_nodes);

    /**
     * \brief processing logic of Terminate message
     */
    void ProcessTerminateCommand();

    /**
     * \brief processing logic of AddNode message (run on each node)
     */
    void ProcessAddNodeCommand(Message* msg, Meta* nodes, Meta* recovery_nodes);

    /**
     * \brief processing logic of Barrier message (run on each node)
     */
    void ProcessBarrierCommand(Message* msg);

    /**
     * \brief processing logic of AddNode message (run on each node)
     */
    void ProcessHearbeat(Message* msg);

    /**
     * \brief processing logic of Data message
     */
    void ProcessDataMsg(Message* msg);

    /**
     * \brief called by ProcessAddNodeCommand, in scheduler it assigns an id to the
     *        newly added node; in other nodes, it updates the node id with what is received
     *        from scheduler
     */
    void UpdateLocalID(Message* msg, std::unordered_set<int>* deadnodes_set, Meta* nodes,
                       Meta* recovery_nodes);

    const char *heartbeat_timeout_val = Environment::Get()->find("PS_HEARTBEAT_TIMEOUT");
    int heartbeat_timeout_ = heartbeat_timeout_val ? atoi(heartbeat_timeout_val) : 0;

    DISALLOW_COPY_AND_ASSIGN(Van);
};
}  // namespace ps
#endif  // PS_INTERNAL_VAN_H_
