/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_MESSAGE_H_
#define PS_INTERNAL_MESSAGE_H_
#include <vector>
#include <limits>
#include <string>
#include <sstream>
#include "ps/sarray.h"


#define LITTLE_GRAIN_MSG
#define UDP_CHANNEL
#define DOUBLE_CHANNEL
#define TCP_MIN_MSG_SIZE 4096*400
#ifndef MAX_MSG_LIMIT
#define MAX_MSG_LIMIT 1*1024
#endif
#ifndef EVAL_CONTRIBUTE_CON
#define EVAL_CONTRIBUTE_CON
#endif
#ifndef RECONSTRUCT
#define RECONSTRUCT
#endif
namespace ps {
/** \brief data type */
enum DataType {
  CHAR, INT8, INT16, INT32, INT64,
  UINT8, UINT16, UINT32, UINT64,
  FLOAT, DOUBLE, OTHER
};
/** \brief data type name */
static const char* DataTypeName[] = {
  "CHAR", "INT8", "INT16", "INT32", "INT64",
  "UINT8", "UINT16", "UINT32", "UINT64",
  "FLOAT", "DOUBLE", "OTHER"
};
/**
 * \brief compare if V and W are the same type
 */
template<typename V, typename W>
inline bool SameType() {
  return std::is_same<typename std::remove_cv<V>::type, W>::value;
}
/**
 * \brief return the DataType of V
 */
template<typename V>
DataType GetDataType() {
  if (SameType<V, int8_t>()) {
    return INT8;
  } else if (SameType<V, int16_t>()) {
    return INT16;
  } else if (SameType<V, int32_t>()) {
    return INT32;
  } else if (SameType<V, int64_t>()) {
    return INT64;
  } else if (SameType<V, uint8_t>()) {
    return UINT8;
  } else if (SameType<V, uint16_t>()) {
    return UINT16;
  } else if (SameType<V, uint32_t>()) {
    return UINT32;
  } else if (SameType<V, uint64_t>()) {
    return UINT64;
  } else if (SameType<V, float>()) {
    return FLOAT;
  } else if (SameType<V, double>()) {
    return DOUBLE;
  } else {
    return OTHER;
  }
}
/**
 * \brief information about a node
 */
struct Node {
  /** \brief the empty value */
  static const int kEmpty;
  /** \brief default constructor */
#ifdef UDP_CHANNEL
  Node() : id(kEmpty), port(kEmpty), is_recovery(false) {}
#else
  Node() : id(kEmpty), port(kEmpty), is_recovery(false) {}
#endif
  /** \brief node roles */
  enum Role { SERVER, WORKER, SCHEDULER };
  /** \brief get debug string */
  std::string DebugString() const {
    std::stringstream ss;
    ss << "role=" << (role == SERVER ? "server" : (role == WORKER ? "worker" : "scheduler"))
       << (id != kEmpty ? ", id=" + std::to_string(id) : "")
       << ", ip=" << hostname << ", port=" << port <<", is_recovery=" << is_recovery;
    for(int i = 0; i< udp_port.size(); ++i){
        ss << "udp[channel "<< i+1 << "] port = " << udp_port[i];
    }
    return ss.str();
  }
  /** \brief get short debug string */
  std::string ShortDebugString() const {
    std::string str = role == SERVER ? "S" : (role == WORKER ? "W" : "H");
    if (id != kEmpty) str += "[" + std::to_string(id) + "]";
    return str;
  }
  /** \brief the role of this node */
  Role role;
  /** \brief node id */
  int id;
  /** \brief customer id */
  int customer_id;
  /** \brief hostname or ip */
  std::string hostname;
  /** \brief the port this node is binding */
  int port;
#ifdef DOUBLE_CHANNEL
  /** \brief the udp port this node is binding */
  //int udp_port;
  //int udp_port_ch2;
  //int udp_port_ch3;
  std::vector<int> udp_port;
#endif
  /** \brief whether this node is created by failover */
  bool is_recovery;
};
/**
 * \brief meta info of a system control message
 */
struct Control {
  /** \brief empty constructor */
  Control() : cmd(EMPTY) { }
  /** \brief return true is empty */
  inline bool empty() const { return cmd == EMPTY; }
  /** \brief get debug string */
  std::string DebugString() const {
    if (empty()) return "";
    std::vector<std::string> cmds = {
      "EMPTY", "TERMINATE", "ADD_NODE", "BARRIER", "ACK", "HEARTBEAT", "DATA"};
    std::stringstream ss;
    ss << "cmd=" << cmds[cmd];
    if (node.size()) {
      ss << ", node={";
      for (const Node& n : node) ss << " " << n.DebugString();
      ss << " }";
    }
    if (cmd == BARRIER) ss << ", barrier_group=" << barrier_group;
    if (cmd == ACK) ss << ", msg_sig=" << msg_sig;
    return ss.str();
  }
  /** \brief all commands */
  enum Command { EMPTY, TERMINATE, ADD_NODE, BARRIER, ACK, HEARTBEAT, DATA };
  /** \brief the command */
  Command cmd;
  /** \brief node infos */
  std::vector<Node> node;
  /** \brief the node group for a barrier, such as kWorkerGroup */
  int barrier_group;
  /** message signature */
  uint64_t msg_sig;
};
#ifdef UDP_CHANNEL
typedef int Meta_head;
#endif
/**
 * \brief meta info of a message
 */
struct Meta {
  /** \brief the empty value */
  static const int kEmpty;
  /** \brief default constructor */
#ifdef UDP_CHANNEL
  Meta() : head(kEmpty), app_id(kEmpty), customer_id(kEmpty),
           timestamp(kEmpty),keys_len(0),vals_len(0),lens_len(0),seq(0),seq_begin(0),seq_end(0), udp_reliable(false),channel(0),msg_type(-1),val_bytes(0), total_bytes(0),sender(kEmpty), recver(kEmpty),
           request(false), push(false), simple_app(false) {}
#else
	 Meta() : head(kEmpty), app_id(kEmpty), customer_id(kEmpty),
           timestamp(kEmpty), sender(kEmpty), recver(kEmpty),
           request(false), push(false), simple_app(false){}
#endif
  std::string DebugString() const {
    std::stringstream ss;
    if (sender == Node::kEmpty) {
      ss << "?";
    } else {
      ss << sender;
    }
    ss <<  " => " << recver;
    ss << ". Meta: request=" << request;
    if (timestamp != kEmpty) ss << ", timestamp=" << timestamp;
#ifdef LITTLE_GRAIN_MSG
	ss << ", tracker_num = " << tracker_num;
#endif
#ifdef UDP_CHANNEL
    ss << ", first_key = " << first_key;
	ss << ", keys_len = " << keys_len;
	ss << ", vals_len = " << vals_len;
	ss << ", lens_len = " << lens_len;
    ss << ", seq = " << seq;
	ss << ", seq_begin = " << seq_begin;
	ss << ", seq_end = " << seq_end;
	ss << ", udp_reliable = " << udp_reliable;
    ss << ", channel = " << channel;
    ss << ", msg_type = " << msg_type;
    ss << ", push_op_num = " << push_op_num;
    ss << ", val_bytes = " << val_bytes;
    ss << ", total_bytes = " << total_bytes;
    if(compr.size()){
        ss << ", compr = [";
        for(auto v : compr) ss << " " << v;
        ss << " ]";
    }
#endif
  

    if (!control.empty()) {
      ss << ", control={ " << control.DebugString() << " }";
    } else {
      ss << ", app_id=" << app_id
         << ", customer_id=" << customer_id
         << ", simple_app=" << simple_app
         << ", push=" << push;
    }
    if (head != kEmpty) ss << ", head=" << head;
    if (body.size()) ss << ", body=" << body;
    if (data_type.size()) {
      ss << ", data_type={";
      for (auto d : data_type) ss << " " << DataTypeName[static_cast<int>(d)];
      ss << " }";
    }
    return ss.str();
  }
  /** \brief an int head */
  int head;
  /** \brief the unique id of the application of messsage is for*/
  int app_id;
  /** \brief customer id*/
  int customer_id;
  /** \brief the timestamp of this message */
  int timestamp;
#ifdef LITTLE_GRAIN_MSG
  /** \brief bring the tracker_num of timestamp*/
  int tracker_num;
#endif
#ifdef UDP_CHANNEL
  int first_key;   //used for calculate resender_key
  int keys_len;
  int vals_len;
  int lens_len;
  //std::vector<int> data_len;
  int seq;
  int seq_begin;
  int seq_end;
  bool udp_reliable;
  std::vector<float> compr;
  int msg_type;     //point that the type of msg, push:paramter: 1 gradient/update:2 pull: request:3 default:0
  int push_op_num;
  int val_bytes;
  int total_bytes;
#endif

  int channel;

  /** \brief the node id of the sender of this message */
  int sender;
  /** \brief the node id of the receiver of this message */
  int recver;
  /** \brief whether or not this is a request message*/
  bool request;
  /** \brief whether or not a push message */
  bool push;
  /** \brief whether or not it's for SimpleApp */
  bool simple_app;
  /** \brief an string body */
  std::string body;
  /** \brief data type of message.data[i] */
  std::vector<DataType> data_type;
  /** \brief system control message */
  Control control;
};
// rank struct, assist to msg's contribution rank
struct Message_RU{
    int index;
    float contri;
};
// channel manage struct
struct Channel_MS{
    int push_op_num = 0;
    std::unordered_map<int,bool> item;  //int:key_end in layer, bool:point out the channel is open or close;
};
/**
 * \brief messages that communicated amaong nodes.
 */
struct Message {
#ifdef EVAL_CONTRIBUTE_CON
  /*\brief contri*/
  float contri;
  float p_loss;
  int rank;
#endif
  /** \brief the meta info of this message */
  Meta meta;
  /** \brief the large chunk of data of this message */
  std::vector<SArray<char> > data;
  /**
   * \brief push array into data, and add the data type
   */
  template <typename V>
  void AddData(const SArray<V>& val) {
    CHECK_EQ(data.size(), meta.data_type.size());
    meta.data_type.push_back(GetDataType<V>());
    data.push_back(SArray<char>(val));
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body:";
      for (const auto& d : data) ss << " data_size=" << d.size();
    }
    return ss.str();
  }
};
}  // namespace ps
#endif  // PS_INTERNAL_MESSAGE_H_
