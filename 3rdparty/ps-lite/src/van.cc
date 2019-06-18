/**
 *  Copyright (c) 2015 by Contributors
 */
#include "ps/internal/van.h"
#include <thread>
#include <chrono>
#include "ps/base.h"
#include "ps/sarray.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/customer.h"
#include "./network_utils.h"
#include "./meta.pb.h"
#include "./zmq_van.h"
#include "./resender.h"
namespace ps {

// interval in second between to heartbeast signals. 0 means no heartbeat.
// don't send heartbeast in default. because if the scheduler received a
// heartbeart signal from a node before connected to that node, then it could be
// problem.
static const int kDefaultHeartbeatInterval = 0;

Van* Van::Create(const std::string& type) {
  if (type == "zmq") {
    return new ZMQVan();
  } else {
    LOG(FATAL) << "unsupported van type: " << type;
    return nullptr;
  }
}

void Van::ProcessTerminateCommand() {
  PS_VLOG(1) << my_node().ShortDebugString() << " is stopped";
  ready_ = false;
}

void Van::ProcessAddNodeCommandAtScheduler(
        Message* msg, Meta* nodes, Meta* recovery_nodes) {
  recovery_nodes->control.cmd = Control::ADD_NODE;
  time_t t = time(NULL);
  size_t num_nodes = Postoffice::Get()->num_servers() + Postoffice::Get()->num_workers();
  if (nodes->control.node.size() == num_nodes) {
    // sort the nodes according their ip and port,
    std::sort(nodes->control.node.begin(), nodes->control.node.end(),
              [](const Node& a, const Node& b) {
                  return (a.hostname.compare(b.hostname) | (a.port < b.port)) > 0;
              });
    // assign node rank
    for (auto& node : nodes->control.node) {
      std::string node_host_ip = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(node_host_ip) == connected_nodes_.end()) {
        CHECK_EQ(node.id, Node::kEmpty);
        int id = node.role == Node::SERVER ?
                 Postoffice::ServerRankToID(num_servers_) :
                 Postoffice::WorkerRankToID(num_workers_);
        PS_VLOG(1) << "assign rank=" << id << " to node " << node.DebugString();
        node.id = id;
#ifdef DOUBLE_CHANNEL
		Connect(node);
		//Connect_UDP(node);
#else  
        Connect(node);
#endif
        Postoffice::Get()->UpdateHeartbeat(node.id, t);
        connected_nodes_[node_host_ip] = id;
      } else {
        int id = node.role == Node::SERVER ?
                 Postoffice::ServerRankToID(num_servers_) :
                 Postoffice::WorkerRankToID(num_workers_);
        shared_node_mapping_[id] = connected_nodes_[node_host_ip];
        node.id = connected_nodes_[node_host_ip];
      }
      if (node.role == Node::SERVER) num_servers_++;
      if (node.role == Node::WORKER) num_workers_++;
    }
    nodes->control.node.push_back(my_node_);
    nodes->control.cmd = Control::ADD_NODE;
    Message back;
    back.meta = *nodes;
    for (int r : Postoffice::Get()->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      int recver_id = r;
      if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
        back.meta.recver = recver_id;
        back.meta.timestamp = timestamp_++;
        Send(back);
      }
    }
    PS_VLOG(1) << "the scheduler is connected to "
               << num_workers_ << " workers and " << num_servers_ << " servers";
    ready_ = true;
  } else if (!recovery_nodes->control.node.empty()) {
    auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_);
    std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
    // send back the recovery node
    CHECK_EQ(recovery_nodes->control.node.size(), 1);
    Connect(recovery_nodes->control.node[0]);
    Postoffice::Get()->UpdateHeartbeat(recovery_nodes->control.node[0].id, t);
    Message back;
    for (int r : Postoffice::Get()->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      if (r != recovery_nodes->control.node[0].id
          && dead_set.find(r) != dead_set.end()) {
        // do not try to send anything to dead node
        continue;
      }
      // only send recovery_node to nodes already exist
      // but send all nodes to the recovery_node
      back.meta = (r == recovery_nodes->control.node[0].id) ? *nodes : *recovery_nodes;
      back.meta.recver = r;
      back.meta.timestamp = timestamp_++;
      Send(back);
    }
  }
}

void Van::UpdateLocalID(Message* msg, std::unordered_set<int>* deadnodes_set,
                        Meta* nodes, Meta* recovery_nodes) {
  auto& ctrl = msg->meta.control;
  int num_nodes = Postoffice::Get()->num_servers() + Postoffice::Get()->num_workers();
  // assign an id
  if (msg->meta.sender == Meta::kEmpty) {
    CHECK(is_scheduler_);
    CHECK_EQ(ctrl.node.size(), 1);
    if (nodes->control.node.size() < num_nodes) {
      nodes->control.node.push_back(ctrl.node[0]);
    } else {
      // some node dies and restarts
      CHECK(ready_.load());
      for (size_t i = 0; i < nodes->control.node.size() - 1; ++i) {
        const auto& node = nodes->control.node[i];
        if (deadnodes_set->find(node.id) != deadnodes_set->end() &&
            node.role == ctrl.node[0].role) {
          auto& recovery_node = ctrl.node[0];
          // assign previous node id
          recovery_node.id = node.id;
          recovery_node.is_recovery = true;
          PS_VLOG(1) << "replace dead node " << node.DebugString()
                     << " by node " << recovery_node.DebugString();
          nodes->control.node[i] = recovery_node;
          recovery_nodes->control.node.push_back(recovery_node);
          break;
        }
      }
    }
  }

  // update my id
  for (size_t i = 0; i < ctrl.node.size(); ++i) {
    const auto& node = ctrl.node[i];
    if (my_node_.hostname == node.hostname && my_node_.port == node.port) {
    if (getenv("DMLC_RANK") == nullptr || my_node_.id == Meta::kEmpty) {
        my_node_ = node;
        std::string rank = std::to_string(Postoffice::IDtoRank(node.id));
#ifdef _MSC_VER
        _putenv_s("DMLC_RANK", rank.c_str());
#else
        setenv("DMLC_RANK", rank.c_str(), true);
#endif
      }
    }
  }
}

void Van::ProcessHearbeat(Message* msg) {
  auto& ctrl = msg->meta.control;
  time_t t = time(NULL);
  for (auto &node : ctrl.node) {
    Postoffice::Get()->UpdateHeartbeat(node.id, t);
    if (is_scheduler_) {
      Message heartbeat_ack;
      heartbeat_ack.meta.recver = node.id;
      heartbeat_ack.meta.control.cmd = Control::HEARTBEAT;
      heartbeat_ack.meta.control.node.push_back(my_node_);
      heartbeat_ack.meta.timestamp = timestamp_++;
      // send back heartbeat
      Send(heartbeat_ack);
    }
  }
}

void Van::ProcessBarrierCommand(Message* msg) {
  auto& ctrl = msg->meta.control;
  if (msg->meta.request) {
    if (barrier_count_.empty()) {
      barrier_count_.resize(8, 0);
    }
    int group = ctrl.barrier_group;
    ++barrier_count_[group];
    PS_VLOG(1) << "Barrier count for " << group << " : " << barrier_count_[group];
    if (barrier_count_[group] ==
        static_cast<int>(Postoffice::Get()->GetNodeIDs(group).size())) {
      barrier_count_[group] = 0;
      Message res;
      res.meta.request = false;
      res.meta.app_id = msg->meta.app_id;
      res.meta.customer_id = msg->meta.customer_id;
      res.meta.control.cmd = Control::BARRIER;
      for (int r : Postoffice::Get()->GetNodeIDs(group)) {
        int recver_id = r;
        if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
          res.meta.recver = recver_id;
          res.meta.timestamp = timestamp_++;
          CHECK_GT(Send(res), 0);
        }
      }
    }
  } else {
    Postoffice::Get()->Manage(*msg);
  }
}

void Van::ProcessDataMsg(Message* msg) {
  // data msg
#ifdef DOUBLE_CHANNEL
  std::lock_guard<std::mutex> lk(mu_);
#endif
  CHECK_NE(msg->meta.sender, Meta::kEmpty);
  CHECK_NE(msg->meta.recver, Meta::kEmpty);
  CHECK_NE(msg->meta.app_id, Meta::kEmpty);
  int app_id = msg->meta.app_id;
  int customer_id = Postoffice::Get()->is_worker() ? msg->meta.customer_id : app_id;
  auto* obj = Postoffice::Get()->GetCustomer(app_id, customer_id, 5);
  CHECK(obj) << "timeout (5 sec) to wait App " << app_id << " customer " << customer_id \
    << " ready at " << my_node_.role;
  	 if(msg->data.size() > 0){
		  if(msg->data[0][0] > 100000){
				std::cout << "Van->ProcessDataMsg#221:find error key" << msg->data[0] << msg->meta.DebugString() << std::endl;
		   }
		  
	  }
  obj->Accept(*msg);
}

void Van::ProcessAddNodeCommand(Message* msg, Meta* nodes, Meta* recovery_nodes) {
  auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_);
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
  auto& ctrl = msg->meta.control;

  UpdateLocalID(msg, &dead_set, nodes, recovery_nodes);

  if (is_scheduler_) {
    ProcessAddNodeCommandAtScheduler(msg, nodes, recovery_nodes);
  } else {
    for (const auto& node : ctrl.node) {
      std::string addr_str = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(addr_str) == connected_nodes_.end()) {
#ifdef DOUBLE_CHANNEL
	   Connect(node);
       if(node.role != Node::Role::SCHEDULER)
            Connect_UDP(node);
#else
        Connect(node);
#endif
        connected_nodes_[addr_str] = node.id;
      }
      if (!node.is_recovery && node.role == Node::SERVER) ++num_servers_;
      if (!node.is_recovery && node.role == Node::WORKER) ++num_workers_;
    }
    PS_VLOG(1) << my_node_.ShortDebugString() << " is connected to others";
    ready_ = true;
  }
}

void Van::Start(int customer_id) {
  // get scheduler info
  start_mu_.lock();
#ifdef DOUBLE_CHANNEL
   int udp_ch_num = 0;
#endif
  if (init_stage == 0) {
    scheduler_.hostname = std::string(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_URI")));
    scheduler_.port = atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_PORT")));
#ifdef DOUBLE_CHANNEL
    //scheduler_.udp_port = atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_UDP_PORT")));
#endif
    scheduler_.role = Node::SCHEDULER;
    scheduler_.id = kScheduler;
    is_scheduler_ = Postoffice::Get()->is_scheduler();

    // get my node info
    if (is_scheduler_) {
      my_node_ = scheduler_;
    } else {
      auto role = is_scheduler_ ? Node::SCHEDULER :
                  (Postoffice::Get()->is_worker() ? Node::WORKER : Node::SERVER);
      const char *nhost = Environment::Get()->find("DMLC_NODE_HOST");
      std::string ip;
      if (nhost) ip = std::string(nhost);
      if (ip.empty()) {
        const char *itf = Environment::Get()->find("DMLC_INTERFACE");
        std::string interface;
        if (itf) interface = std::string(itf);
        if (interface.size()) {
          GetIP(interface, &ip);
        } else {
          GetAvailableInterfaceAndIP(&interface, &ip);
        }
        CHECK(!interface.empty()) << "failed to get the interface";
      }
      int port = GetAvailablePort();

      const char *pstr = Environment::Get()->find("PORT");
      if (pstr) port = atoi(pstr);
      CHECK(!ip.empty()) << "failed to get ip";
      CHECK(port) << "failed to get a port";
      my_node_.hostname = ip;
      my_node_.role = role;
      my_node_.port = port;
#ifdef DOUBLE_CHANNEL
      
      if(!is_scheduler_){
         udp_ch_num = atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_UDP_CHANNEL_NUM")));
         std::cout << "udp_ch_num = " << udp_ch_num << std::endl;
          for(int i = 0; i < udp_ch_num; ++i){
              int p = GetAvailablePort();
              my_node_.udp_port.push_back(p);
            }  

      }
#endif
      // cannot determine my id now, the scheduler will assign it later
      // set it explicitly to make re-register within a same process possible
      my_node_.id = Node::kEmpty;
      my_node_.customer_id = customer_id;
    }

    // bind.
    my_node_.port = Bind(my_node_, is_scheduler_ ? 0 : 40);
    std::cout << "DEBUG: #323" << std::endl;
    PS_VLOG(1) << "Bind to " << my_node_.DebugString();
    CHECK_NE(my_node_.port, -1) << "TCP:bind failed";
	
#ifdef DOUBLE_CHANNEL
    if(!is_scheduler_){
        my_node_.udp_port = Bind_UDP(my_node_, is_scheduler_ ? 0 : 40);
        PS_VLOG(1) << "(UDP)Bind to " << my_node_.DebugString();
    }
   // CHECK_NE(my_node_.port, -1) << "UDP:bind failed";
#endif
    // connect to the scheduler
    Connect(scheduler_);
#ifdef DOUBLE_CHANNEL
    /* if(!is_scheduler)
        Connect_UDP(scheduler_); */
#endif
    // for debug use
    if (Environment::Get()->find("PS_DROP_MSG")) {
      drop_rate_ = atoi(Environment::Get()->find("PS_DROP_MSG"));
    }
	//std::cout << "#325 at "<< my_node_.DebugString() << std::endl;
#ifdef DOUBLE_CHANNEL
   // start tcp receiver
    tcp_receiver_thread_ = std::unique_ptr<std::thread>(
            new std::thread(&Van::Receiving_TCP, this));
    if(!is_scheduler_){
        // start udp receiver
        for(int i = 0; i < my_node_.udp_port.size(); ++i){
            udp_receiver_thread_[i] = std::unique_ptr<std::thread>(
              new std::thread(&Van::Receiving_UDP,this,i));
           // udp_receiver_thread_vec.push_back(udp_receiver_thread_);
        }
    }
#else
	
    // start receiver
    receiver_thread_ = std::unique_ptr<std::thread>(
            new std::thread(&Van::Receiving, this));
#endif
	//std::cout << "#334 at "<< my_node_.DebugString() << std::endl;
    init_stage++;
  }
  start_mu_.unlock();

  if (!is_scheduler_) {
    // let the scheduler know myself
    Message msg;
    Node customer_specific_node = my_node_;
    customer_specific_node.customer_id = customer_id;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::ADD_NODE;
    msg.meta.control.node.push_back(customer_specific_node);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  start_mu_.lock();
  if (init_stage == 1) {
    // resender
#ifdef UDP_CHANNEL
      int timeout = 100;
	  std::cout << my_node_.role << ":" << "start resender_" << std::endl;
      resender_ = new Resender(timeout, 10, this);
#ifdef CHANNEL_MLR
      std::vector<float> mlr(udp_ch_num,0.5);          
      resender_->init_channel_info(udp_ch_num, mlr);
#endif
#else
    if (Environment::Get()->find("PS_RESEND") && atoi(Environment::Get()->find("PS_RESEND")) != 0) {
      int timeout = 1000;
      if (Environment::Get()->find("PS_RESEND_TIMEOUT")) {
        timeout = atoi(Environment::Get()->find("PS_RESEND_TIMEOUT"));
      }
	  std::cout << my_node_.role << ":" << "start resender_" << std::endl;
      resender_ = new Resender(timeout, 10, this);
    }
#endif
    if (!is_scheduler_) {
      // start heartbeat thread
      heartbeat_thread_ = std::unique_ptr<std::thread>(
              new std::thread(&Van::Heartbeat, this));
    }
    init_stage++;
  }
  start_mu_.unlock();
  std::cout <<my_node_.DebugString() << "start success !!";
}

void Van::Stop() {
  // stop threads
  Message exit;
  exit.meta.control.cmd = Control::TERMINATE;
  exit.meta.recver = my_node_.id;
  // only customer 0 would call this method
  exit.meta.customer_id = 0;
#ifdef DOUBLE_CHANNEL
  int ret = SendMsg_TCP(exit);
#else
  int ret = SendMsg(exit);
#endif
  CHECK_NE(ret, -1);
#ifdef DOUBLE_CHANNEL
	tcp_receiver_thread_->join();
    for(int i = 0; i < udp_receiver_thread_vec.size(); ++i){
        udp_receiver_thread_[i]->join();
    }
#else
  receiver_thread_->join();
#endif
  init_stage = 0;
  if (!is_scheduler_) heartbeat_thread_->join();
  if (resender_) delete resender_;
  ready_ = false;
  connected_nodes_.clear();
  shared_node_mapping_.clear();
  send_bytes_ = 0;
  timestamp_ = 0;
  my_node_.id = Meta::kEmpty;
  barrier_count_.clear();
}
#ifdef DOUBLE_CHANNEL
#ifdef ADAPTIVE_K
float Van::Average_tp(){
    float avg_tp = resender_->get_average_throughput();
   // std::cout << "avg_tp = " << avg_tp;
    return avg_tp;
}
#endif
int Van::Send( Message& msg, int channel, int tag) {
	int send_bytes = 0;
    

  if(channel == 0){
#ifdef ADAPTIVE_K
    if(msg.meta.push && msg.meta.request){
          if(msg.meta.first_key == 0) monitor_count = 0;
         /*  unsigned seed = time(NULL) + my_node_.id;
          float monitor_rate = 20; 
          if (rand_r(&seed) % 100 < monitor_rate) {
            msg.meta.udp_reliable=1;
          }*/
          if (monitor_count % 100 == 0) {
            msg.meta.udp_reliable=1;
          }
          if(msg.meta.udp_reliable){
              //std::cout << "ready to monitor this message" << std::endl;
              resender_->AddOutgoing(msg);
          }
          monitor_count++;
      } 
#endif
	  send_bytes = SendMsg_TCP(msg, tag);

  }else{
#ifdef CHANNEL_MLR   
    if(msg.meta.push ){
         for(int c = 0; c < resender_-> get_channel_num(); ++c){
            //std::cout << "lr[ "<< c << "] = " << resender_-> get_realtime_lr(c) << std::endl; 
            resender_-> get_realtime_lr(c);
        }
    }   
#endif 
	  send_bytes = SendMsg_UDP(channel-1, msg, tag);
      
	  if(msg.meta.udp_reliable){
		  if (resender_) {
		  //std::cout << "Van::Send, record outgoing msg" << std::endl;
          
		  resender_->AddOutgoing(msg);

		}
	  }
	  /* static uint64_t udp_snd_msg_cnt = 0;
	  if(send_bytes > 0) udp_snd_msg_cnt++;
	  if(msg.meta.udp_reliable){
		  std::cout << " udp_snd_msg_cnt = " <<  udp_snd_msg_cnt << std::endl;
		  udp_snd_msg_cnt = 0;
	  } */
	
  }
  //std::cout << "Van::Send, send " << send_bytes << "bytes" << std::endl;
  CHECK_NE(send_bytes, -1);
  send_bytes_ += send_bytes;
  
  if (Postoffice::Get()->verbose() >= 2) {
    PS_VLOG(2) << "send_bytes = "<<send_bytes<<msg.DebugString();
  }
  return send_bytes;
}
#ifdef CHANNEL_MLR
void Van::Update_Sendbuff( int timestamp) {
    if (resender_){
		  resender_->Update_Sendbuff(timestamp);
		}
}
#endif
#else
int Van::Send( Message& msg) {
  static unsigned int count = 0;
  count++;
  //std::cout << "Van::Send:count = " << count << std::endl;
  int send_bytes = SendMsg(msg);
  //std::cout << "Van::Send, send " << send_bytes << "bytes" << std::endl;
  CHECK_NE(send_bytes, -1);
  send_bytes_ += send_bytes;
  if (resender_) {
	  //std::cout << "Van::Send, record outgoing msg" << std::endl;
	  resender_->AddOutgoing(msg);
  }
  if (Postoffice::Get()->verbose() >= 2) {
    PS_VLOG(2) << msg.DebugString();
  }
  return send_bytes;
}
#endif

/*if DOUBLE_CHANNEL, Receiving => Receiving_TCP and Receiving_UDP*/
#ifdef DOUBLE_CHANNEL
void Van::Receiving_TCP() {
  Meta nodes;
  Meta recovery_nodes;  // store recovery nodes
  recovery_nodes.control.cmd = Control::ADD_NODE;

  while (true) {
    Message msg;
    int recv_bytes = RecvMsg_TCP(&msg);
	
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }

    CHECK_NE(recv_bytes, -1);
    recv_bytes_ += recv_bytes;
    if (Postoffice::Get()->verbose() >= 2) {
      PS_VLOG(2) << msg.DebugString();
    }
	
    // duplicated message
    //if (resender_ && resender_->AddIncomming(msg)) continue;
	/* if(msg.meta.push && msg.meta.request){
		if (resender_ && resender_->AddIncomming_Push(msg)) continue;
	}  */
    /*server run*/
    if(msg.meta.push && msg.meta.request){
        //std::cout << "get a push&& request msg" << std::endl;
        if(msg.meta.udp_reliable){
            if (resender_ && resender_->AddIncomming(msg)) continue;
        }
	}
    /*worker run*/
    if(msg.meta.control.cmd == Control::ACK){
        //std::cout << "get an  ACK" << std::endl;
		if (resender_ && resender_->AddIncomming(msg)) continue;
	}

	if (!msg.meta.control.empty()) {
      // control msg
      auto& ctrl = msg.meta.control;
      if (ctrl.cmd == Control::TERMINATE) {
        ProcessTerminateCommand();
        break;
      } else if (ctrl.cmd == Control::ADD_NODE) {
        ProcessAddNodeCommand(&msg, &nodes, &recovery_nodes);
      } else if (ctrl.cmd == Control::BARRIER) {
        ProcessBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::HEARTBEAT) {
        ProcessHearbeat(&msg);
	  }else {
        LOG(WARNING) << "Drop unknown typed message " << msg.DebugString();
      }
    } else {
      ProcessDataMsg(&msg);
	  /* if(msg.meta.push && msg.meta.request && msg.meta.first_key == msg.meta.key_end){
		  
		  for(std::vector<Resender::Push_Entry>::iterator it = resender_->push_rcv_buff_[msg.meta.sender].begin(); it != resender_->push_rcv_buff_[msg.meta.sender].end(); ++it){
			  if(it->incomming_msg_cnt < resender_->push_rcv_buff_[msg.meta.sender][msg.meta.key_end].incomming_msg_cnt){
				  // means the key's msg has lost
				 if(it->pre_msg_seq != 0){
					   std::cout << "#history:incomming_msg_cnt = " << it->incomming_msg_cnt <<"("<<resender_->push_rcv_buff_[msg.meta.sender][msg.meta.key_end].incomming_msg_cnt << ")" << " Recovery MSG:" << it->pre_msg.DebugString() << std::endl;
					   //ProcessDataMsg(&it->pre_msg); //use the history msg
				 }else{
					 Message tmp_msg;
					 tmp_msg.meta = msg.meta;
					 tmp_msg.meta.first_key = std::distance(resender_->push_rcv_buff_[msg.meta.sender].begin(),it);
					 tmp_msg.meta.keys_len = sizeof(uint64_t);
					 tmp_msg.meta.vals_len = MAX_MSG_LIMIT;
					 tmp_msg.meta.lens_len = sizeof(int);
					 tmp_msg.data.resize(3);
					 uint64_t tmp_key = (uint64_t)tmp_msg.meta.first_key;
					 tmp_msg.data[0].reset((char*)&tmp_key, sizeof(tmp_key), [](char* p){});
					 tmp_msg.data[1].resize(MAX_MSG_LIMIT);
					 tmp_msg.data[2].reset((char*)&tmp_msg.meta.lens_len, sizeof(tmp_msg.meta.lens_len), [](char* p){});
					 std::cout << "#zero:incomming_msg_cnt = " << it->incomming_msg_cnt <<"("<<resender_->push_rcv_buff_[msg.meta.sender][msg.meta.key_end].incomming_msg_cnt << ")" << "Recovery MSG:" << tmp_msg.DebugString() << std::endl;
					 //ProcessDataMsg(&tmp_msg);
					 it->incomming_msg_cnt += 1;
				 }
			  }
		  }
		  
	  } */
    }
  }
}

void Van::Receiving_UDP(int channel) {
  Meta nodes;
  Meta recovery_nodes;  // store recovery nodes
  recovery_nodes.control.cmd = Control::ADD_NODE;
  PS_VLOG(1) << "Start thread{Receiving_UDP} in UDP channel [" << channel+1 << "]";
  while (true) {
    Message msg;
    int recv_bytes = RecvMsg_UDP(channel, &msg);
	
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }

    CHECK_NE(recv_bytes, -1);
    recv_bytes_ += recv_bytes;
    if (Postoffice::Get()->verbose() >= 2) {
      PS_VLOG(2) << msg.DebugString();
    }
	
	 /* static uint64_t udp_rcv_msg_cnt = 0;
	  if(recv_bytes > 0) udp_rcv_msg_cnt++;
	  if(msg.meta.udp_reliable){
		  std::cout << " udp_rcv_msg_cnt = " <<  udp_rcv_msg_cnt << std::endl;
		  udp_rcv_msg_cnt = 0;
	  } */
    // duplicated message
    //LOG(WARNING) << msg.DebugString();
	if(msg.meta.control.cmd == Control::ACK || msg.meta.udp_reliable){
		if (resender_ && resender_->AddIncomming(msg)) continue;
	}
	if (!msg.meta.control.empty()) {
      // control msg
      auto& ctrl = msg.meta.control;
      if (ctrl.cmd == Control::TERMINATE) {
        ProcessTerminateCommand();
        break;
      } else if (ctrl.cmd == Control::ADD_NODE) {
        ProcessAddNodeCommand(&msg, &nodes, &recovery_nodes);
      } else if (ctrl.cmd == Control::BARRIER) {
        ProcessBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::HEARTBEAT) {
        ProcessHearbeat(&msg);
	  }else {
        LOG(WARNING) << "Drop unknown typed message " << msg.DebugString();
      }
    } else {
	  
      ProcessDataMsg(&msg);
	  
	  
    }
  }
}
#else
/*old version Receiving*/
void Van::Receiving() {
  Meta nodes;
  Meta recovery_nodes;  // store recovery nodes
  recovery_nodes.control.cmd = Control::ADD_NODE;

  while (true) {
    Message msg;
    int recv_bytes = RecvMsg(&msg);
	
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }

    CHECK_NE(recv_bytes, -1);
    recv_bytes_ += recv_bytes;
    if (Postoffice::Get()->verbose() >= 2) {
      PS_VLOG(2) << msg.DebugString();
    }
	
    // duplicated message
    if (resender_ && resender_->AddIncomming(msg)) continue;

	if (!msg.meta.control.empty()) {
      // control msg
      auto& ctrl = msg.meta.control;
      if (ctrl.cmd == Control::TERMINATE) {
        ProcessTerminateCommand();
        break;
      } else if (ctrl.cmd == Control::ADD_NODE) {
        ProcessAddNodeCommand(&msg, &nodes, &recovery_nodes);
      } else if (ctrl.cmd == Control::BARRIER) {
        ProcessBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::HEARTBEAT) {
        ProcessHearbeat(&msg);
	  }else {
        LOG(WARNING) << "Drop unknown typed message " << msg.DebugString();
      }
    } else {
	  
      ProcessDataMsg(&msg);
	  
    }
  }
}
#endif
void Van::PackMeta(const Meta& meta, char** meta_buf, int* buf_size) {
  // convert into protobuf
  PBMeta pb;
  pb.set_head(meta.head);
  if (meta.app_id != Meta::kEmpty) pb.set_app_id(meta.app_id);
  if (meta.timestamp != Meta::kEmpty) pb.set_timestamp(meta.timestamp);
  if (meta.body.size()) pb.set_body(meta.body);
  /*tracker_num used for little msg*/
  pb.set_tracker_num(meta.tracker_num);
#ifdef UDP_CHANNEL
  pb.set_keys_len(meta.keys_len);
  pb.set_vals_len(meta.vals_len);
  pb.set_lens_len(meta.lens_len);
  
  pb.set_sender(my_node_.id);
  pb.set_recver(meta.recver);
  pb.set_first_key(meta.first_key);
  
  pb.set_key_begin(meta.key_begin);
  pb.set_key_end(meta.key_end);
  pb.set_udp_reliable(meta.udp_reliable);
  pb.set_channel(meta.channel);
  
#endif
  pb.set_push(meta.push);
  pb.set_request(meta.request);
  pb.set_simple_app(meta.simple_app);
  pb.set_customer_id(meta.customer_id);
  for (auto d : meta.data_type) pb.add_data_type(d);
  if (!meta.control.empty()) {
    auto ctrl = pb.mutable_control();
    ctrl->set_cmd(meta.control.cmd);
    if (meta.control.cmd == Control::BARRIER) {
      ctrl->set_barrier_group(meta.control.barrier_group);
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->set_msg_sig(meta.control.msg_sig);
    }
    for (const auto& n : meta.control.node) {
      auto p = ctrl->add_node();
      p->set_id(n.id);
      p->set_role(n.role);
      p->set_port(n.port);
#ifdef DOUBLE_CHANNEL
      for(auto up : n.udp_port){
          p->add_udp_port(up);
      }
#endif
      p->set_hostname(n.hostname);
      p->set_is_recovery(n.is_recovery);
      p->set_customer_id(n.customer_id);
    }
  }

  // to string
  *buf_size = pb.ByteSize();
  *meta_buf = new char[*buf_size+1];
  CHECK(pb.SerializeToArray(*meta_buf, *buf_size))
    << "failed to serialize protbuf";
}

void Van::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) {
  // to protobuf
  PBMeta pb;
  CHECK(pb.ParseFromArray(meta_buf, buf_size))
    << "failed to parse string into protobuf";

  // to meta
  meta->head = pb.head();
  meta->app_id = pb.has_app_id() ? pb.app_id() : Meta::kEmpty;
  meta->timestamp = pb.has_timestamp() ? pb.timestamp() : Meta::kEmpty;
  /*tracker_num used for little grain message*/
  meta->tracker_num = pb.tracker_num();
#ifdef UDP_CHANNEL
  meta->keys_len = pb.keys_len();
  meta->vals_len = pb.vals_len();
  meta->lens_len = pb.lens_len();
  meta->sender = pb.sender();
  meta->recver = pb.recver();
  meta->first_key = pb.first_key();
  
  meta->key_begin = pb.key_begin();
  meta->key_end = pb.key_end();
  
  meta->udp_reliable = pb.udp_reliable();
  meta->channel = pb.channel();
#endif
  meta->request = pb.request();
  meta->push = pb.push();
  meta->simple_app = pb.simple_app();
  meta->body = pb.body();
  meta->customer_id = pb.customer_id();
  meta->data_type.resize(pb.data_type_size());
  for (int i = 0; i < pb.data_type_size(); ++i) {
    meta->data_type[i] = static_cast<DataType>(pb.data_type(i));
  }
  if (pb.has_control()) {
    const auto& ctrl = pb.control();
    meta->control.cmd = static_cast<Control::Command>(ctrl.cmd());
    meta->control.barrier_group = ctrl.barrier_group();
    meta->control.msg_sig = ctrl.msg_sig();
    for (int i = 0; i < ctrl.node_size(); ++i) {
      const auto& p = ctrl.node(i);
      Node n;
      n.role = static_cast<Node::Role>(p.role());
      n.port = p.port();
#ifdef DOUBLE_CHANNEL
      for(int i = 0; i < p.udp_port_size();++i){
          n.udp_port.push_back(p.udp_port(i));
      }
#endif
      n.hostname = p.hostname();
      n.id = p.has_id() ? p.id() : Node::kEmpty;
      n.is_recovery = p.is_recovery();
      n.customer_id = p.customer_id();
      meta->control.node.push_back(n);
    }
  } else {
    meta->control.cmd = Control::EMPTY;
  }
 // std::cout << "van.cc:528" << std::endl;
}

void Van::Heartbeat() {
  const char* val = Environment::Get()->find("PS_HEARTBEAT_INTERVAL");
  const int interval = val ? atoi(val) : kDefaultHeartbeatInterval;
  while (interval > 0 && ready_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    Message msg;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::HEARTBEAT;
    msg.meta.control.node.push_back(my_node_);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }
}
}  // namespace ps
