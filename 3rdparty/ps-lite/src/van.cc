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
void Van::MergeMsg(Message* msg1, Message* msg2){
    std::lock_guard<std::mutex> lk(merge_mu_);
    float *p1 = (float*)msg1->data[1].data();
    float *p2 = (float*)msg2->data[1].data();
    int nlen1 = msg1->data[1].size() / sizeof(float);
    int nlen2 = msg2->data[1].size() / sizeof(float);
    assert(nlen1 == nlen2);
    //std::cout << "nlen1=" << nlen1 << ",nlen2=" << nlen2 << std::endl;
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 1;
    float* merged = (float*)malloc(nlen1*sizeof(float));
    float* n = (float*)malloc(nlen1*sizeof(float));
    memcpy(merged,p1,nlen1*sizeof(float));
    memcpy(n,p2,nlen1*sizeof(float));
    for(int i = 0; i < nlen1; i++){
        merged[i] += n[i];
    }
    msg1->data[1].reset((char *)merged,nlen1*sizeof(float), [merged](char* buf) {
                    free(merged);
                    });
    free(n);
}
void Van::ZeroMsg(Message* msg1){
    memset(msg1->data[1].data(),0,msg1->data[1].size());
}
void Van::ZeroSArray(char *sa,int size){
    memset(sa,0,size);
    
}
void Van::MergeSArray(char* sa1, char* sa2, int size){
    float *p1 = (float *)sa1;
    float *p2 = (float *)sa2;
    //std::cout << "before add !" << std::endl;
    for(int i = 0; i < size/sizeof(float); i++){
        //p1[i] = p1[i]+p2[i];
        p1[i] = p2[i];
    }
    // std::cout << "end add !" << std::endl;
    
}
void Van::ReduceSArray(char* sa,int size,int push_var){
    float *p = (float *)sa;
    if(push_var == 0) return;
    
 
    for(int i = 0; i < size/sizeof(float); i++){
        p[i] = ((float)1/(push_var+1))*p[i];
    }
    // std::cout << "end add !" << std::endl;
    
}
bool Van::AsynAccept(Message* msg){
     
  /*   if(recv_map[msg->meta.sender][msg->meta.first_key].find(msg->meta.seq) == recv_map[msg->meta.sender][msg->meta.first_key].end()){
        SArray<char> r(msg->data[1].size());
        recv_map[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = r;
        recv_flag[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = 0;
        meta_map[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = msg->meta;
    } */
    //MergeSArray(recv_map[msg->meta.sender][msg->meta.first_key][msg->meta.seq].data(),msg->data[1].data(),msg->data[1].size());
    //recv_map[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = msg->data[1];
    std::cout << "#272" << std::endl;
    msg_map[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = *msg;
    std::cout << "#274" << std::endl;//
    //recv_flag[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = msg->meta.push_op_num;
    
    if(msg->meta.seq == msg->meta.seq_end){
        int ct = 0;
        int cn = 0;
        int push_t = 0;
        int push_n = 0;
        int cur_push = 0;
        /* msg->data[1].resize(msg->meta.total_bytes);
        char *pdata = msg->data[1].data(); */
        char* buf = (char *)malloc(msg->meta.total_bytes);
        memset(buf,0,msg->meta.total_bytes);
        /*********************************************************test*/
        if(msg->meta.sender == 9)  ct += msg->meta.seq_end+1;
        /*********************************************************/
        
        for(auto &m : msg_map[msg->meta.sender][msg->meta.first_key]){
           // if(recv_flag[msg->meta.sender][msg->meta.first_key][s.first] == 0) continue;
            /*********************************************************test*/
            if(msg->meta.sender == 9) {
                cn += 1;
                if(msg->meta.push_op_num-m.second.meta.push_op_num>0){
                    push_t += msg->meta.push_op_num-m.second.meta.push_op_num;
                    push_n += 1;
                }
                if(msg->meta.push_op_num-m.second.meta.push_op_num==0){
                    cur_push += 1;
                }
                
                
            } 
            /*********************************************************/
            
            ReduceSArray(m.second.data[1].data(),m.second.data[1].size(),msg->meta.push_op_num-m.second.meta.push_op_num);
            std::cout << "#305" << std::endl;
            //memcpy(pdata+m.second.meta.val_bytes, m.second.data[1].data(), m.second.data[1].size());
            memcpy(buf+m.second.meta.val_bytes, m.second.data[1].data(), m.second.data[1].size());
            std::cout << "#307" << std::endl;
            //ZeroSArray(s.second.data(),s.second.size());
            //recv_flag[msg->meta.sender][msg->meta.first_key][s.first] = 0;
        }
        /* msg->data[1].reset(buf, msg->meta.total_bytes, [buf](char* p) {
                            free(buf);
                            }); */
        Message rmsg;
        rmsg.meta = msg->meta;
              
        rmsg.data.push_back(msg->data[0]);
        SArray<char> data;
        data.reset(buf, msg->meta.total_bytes, [buf](char* p) {
                            free(buf);
                            });
        rmsg.data.push_back(data);     
        rmsg.data.push_back(msg->data[2]); 
        /*********************************************************test*/
        if(msg->meta.sender == 9){
            std::cout << "push_op_num = " << msg->meta.push_op_num << ",key = " << msg->meta.first_key << ",cur_push rate =" << "("<< cur_push <<"/" << ct << ")"<< (float)cur_push/ct<<",complete rate = " << "("<< cn <<"/" << ct << ")"<<(float)cn/ct << ",delay avg iter = " << "("<< push_t <<"/" << push_n << ")"<<(float)push_t/push_n << std::endl;
        }  
        /*********************************************************/
        
        return true;
        
    }
    return false;
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
  	 /* if(msg->data.size() > 0){
		  if(msg->data[0][0] > 100000){
				std::cout << "Van->ProcessDataMsg#221:find error key" << msg->data[0] << msg->meta.DebugString() << std::endl;
		   }
		  
	  } */
  //std::cout << msg->DebugString() << std::endl;
  
  if(my_node_.role == 0 && msg->meta.msg_type == 2){   //run only on server side
    if(reconstruct){
        if(msg_map[msg->meta.sender][msg->meta.first_key].find(msg->meta.seq) == msg_map[msg->meta.sender][msg->meta.first_key].end()){
            msg_map[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = *msg;
        }else{
            MergeMsg(&msg_map[msg->meta.sender][msg->meta.first_key][msg->meta.seq],msg);
        }
        
       
        //recv_flag[msg->meta.sender][msg->meta.first_key][msg->meta.seq] = msg->meta.push_op_num;
        
        if(msg->meta.seq == msg->meta.seq_end){
            int ct = 0;
            int cn = 0;
            int push_t = 0;
            int push_n = 0;
            int cur_push = 0;
           /*  msg->data[1].resize(msg->meta.total_bytes);
            char *pdata = msg->data[1].data(); */
            char* buf = (char *)malloc(msg->meta.total_bytes);
            memset(buf,0,msg->meta.total_bytes);
            /* if(msg_buffer[msg->meta.sender].find(msg->meta.first_key) == msg_buffer[msg->meta.sender].end()){
                memset(buf,0,msg->meta.total_bytes);
            }else{
                memcpy(buf,msg_buffer[msg->meta.sender][msg->meta.first_key].data[1].data(),msg->meta.total_bytes);////
            } */
            
            
            /*********************************************************test*/
            if(msg->meta.sender == 9)  ct += msg->meta.seq_end+1;
            /*********************************************************/
            
            for(auto &m : msg_map[msg->meta.sender][msg->meta.first_key]){
               // if(recv_flag[msg->meta.sender][msg->meta.first_key][s.first] == 0) continue;
                /*********************************************************test*/
                if(msg->meta.sender == 9) {
                    cn += 1;
                    if(msg->meta.push_op_num-m.second.meta.push_op_num>0){
                        push_t += msg->meta.push_op_num-m.second.meta.push_op_num;
                        push_n += 1;
                    }
                    if(msg->meta.push_op_num-m.second.meta.push_op_num==0){
                        cur_push += 1;
                    }
 
                } 
                /*********************************************************/
                
                //ReduceSArray(m.second.data[1].data(),m.second.data[1].size(),msg->meta.push_op_num-m.second.meta.push_op_num);
                memcpy(buf+m.second.meta.val_bytes, m.second.data[1].data(), m.second.data[1].size());
                //if(msg->meta.push_op_num-m.second.meta.push_op_num > 5) msg_map[msg->meta.sender][msg->meta.first_key].erase(m.first);
                //ZeroSArray(s.second.data(),s.second.size());
                //recv_flag[msg->meta.sender][msg->meta.first_key][s.first] = 0;
            }
            msg_map[msg->meta.sender][msg->meta.first_key].clear();
            msg->data[1].reset(buf, msg->meta.total_bytes, [buf](char* p) {
                                free(buf);
                                });
            // Message rmsg;
            // rmsg.meta = msg->meta;
                  
            // rmsg.data.push_back(msg->data[0]);
            // SArray<char> data;
            // data.reset(buf, msg->meta.total_bytes, [buf](char* p) {
                                // free(buf);
                                // });
            // rmsg.data.push_back(data);     
            // rmsg.data.push_back(msg->data[2]); 
            /*********************************************************test*/
            if(msg->meta.sender == 9){
                std::cout << "push_op_num = " << msg->meta.push_op_num << ",key = " << msg->meta.first_key << ",cur_push rate =" << "("<< cur_push <<"/" << ct << ")"<< (float)cur_push/ct<<",complete rate = " << "("<< cn <<"/" << ct << ")"<<(float)cn/ct << ",delay avg iter = " << "("<< push_t <<"/" << push_n << ")"<<(float)push_t/push_n << ",tcp_recv = " << tcp_recv << ",udp_recv = " << udp_recv <<std::endl;
            }  
            /*********************************************************/
             obj->Accept(*msg);
             msg_buffer[msg->meta.sender][msg->meta.first_key] = *msg;
        // if(AsynAccept(msg)) obj->Accept(*msg);
        }
    }else{
        obj->Accept(*msg);
    }
  }else{  //run on worker side
      obj->Accept(*msg);
  }
  
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
#ifdef ENCODE
        enable_encode = atoi(CHECK_NOTNULL(Environment::Get()->find("ENABLE_ENCODE")));
        std::cout << "enable_encode = " << enable_encode << std::endl;
#endif
#ifdef RECONSTRUCT
       //msg_size_limit = dmlc::GetEnv("DGT_MSG_SIZE_LIMIT", 4 * 1024);
       reconstruct = atoi(CHECK_NOTNULL(Environment::Get()->find("DGT_RECONSTRUCT")));
       std::cout << "reconstruct[in van.cc] = " << reconstruct << std::endl;
       ns_delay = atoi(CHECK_NOTNULL(Environment::Get()->find("NS_DELAY")));
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
            new std::thread(&Van::Receiving, this));
    if(!is_scheduler_){
        // start udp receiver
        for(int i = 0; i < my_node_.udp_port.size(); ++i){
            udp_receiver_thread_[i] = std::unique_ptr<std::thread>(
              new std::thread(&Van::Receiving_UDP,this,i));
           // udp_receiver_thread_vec.push_back(udp_receiver_thread_);
        }
        important_scheduler_thread_ = std::unique_ptr<std::thread>(
            new std::thread(&Van::Important_scheduler, this));
        unimportant_scheduler_thread_ = std::unique_ptr<std::thread>(
            new std::thread(&Van::Unimportant_scheduler, this));
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
      int timeout = 1000;
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
    if(!is_scheduler_){
        important_scheduler_thread_->join();
        unimportant_scheduler_thread_->join();
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
#ifdef ENCODE
void  Van::msg_float_print(Message& msg, int n){
    float *p = (float*) msg.data[1].data();
    for(int i = 0; i<n; ++i){
        std::cout << " " << *(p+i);
        if(i % 10 == 0) std::cout << std::endl;
    }
    std::cout << std::endl;
}
float Van::encode(Message& msg){
    SArray<char>& s_val = msg.data[1];
    float *ps = (float*)s_val.data();
    int d_val_size = 0;
    assert(s_val.size() == *((int*)msg.data[2].data()));
    if(s_val.size()%16 == 0){
        d_val_size = s_val.size()/16;
    }else{
        d_val_size = s_val.size()/16 + 1;
    }
    SArray<char> d_val(d_val_size);
    char *pd = d_val.data();
    auto it = residual.find(msg.meta.first_key);
    if(it == residual.end()) residual[msg.meta.first_key] = SArray<char>(s_val.size());
    float *pr = (float*)residual[msg.meta.first_key].data();
    
    int param_n = s_val.size() / sizeof(float);
    float max_v = 0.0, min_v = 0.0;
    for(int i = 0; i < param_n; ++i){
        //*(pr+i) += *(ps+i);
        *(pr+i) = *(ps+i);
    }
    for(int i = 0; i < param_n; ++i){
        max_v = std::max(max_v, *(pr+i));
        min_v = std::min(min_v, *(pr+i));
    }
    for(int i = 0; i < 4; ++i){
        float zv = ((min_v + i*(max_v-min_v)/4) + (min_v + (i+1)*(max_v-min_v)/4))/2;
        msg.meta.compr.push_back(zv);
    }
    char qj = 0;
    for(int i = 0; i < param_n; ++i){
        for(int j=0; j < 4; ++j){
            if(*(pr+i) >= min_v + j*(max_v-min_v)/4 && *(pr+i) <= min_v + (j+1)*(max_v-min_v)/4){
                qj = j;
                break;
            }
        }
        *pd |= (qj << ((3-i%4)*2));
        if(i%4 == 0) pd += 1;
        *(pr+i) -= msg.meta.compr[qj];
    } 
    msg.data[1] = d_val;
    msg.meta.vals_len = msg.data[1].size();
}
void Van::decode(Message& msg) {
    SArray<char>& s_val = msg.data[1];
    char *ps = (char*)s_val.data();
    int d_val_size = *((int*)msg.data[2].data());
    SArray<char> d_val(d_val_size);
    float *pd = (float*)d_val.data();
    //int nlen = *((int*)msg.data[2].data());
    int nlen = s_val.size();
    char qj = 0;
    int param_n = d_val_size/sizeof(float);
    for(int i = 0; i < nlen; ++i){
        for(int j =0; j<4; ++j){
            qj = (*(ps+i) >> (6-2*j)) & 0x03;
            *pd = msg.meta.compr[qj];
            pd += 1;
            param_n -= 1;
            if(param_n == 0) break;
        }   
    } 
    msg.data[1] = d_val;
    msg.meta.vals_len = msg.data[1].size();
}
#endif
int Van::Classifier( Message& msg, int channel, int tag) {
    if(channel == 0){
        important_queue_.Push(msg);
    }else{
        unimportant_queue_.Push(msg);
    }
}
void Van::Important_scheduler() {
  while (true) {
    Message msg;
    important_queue_.WaitAndPop(&msg);
    Important_send(msg);
  }
}
void Van::Unimportant_scheduler() {
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = ns_delay;
  while (true) {
    //if(important_queue_.empty()){
        Message msg;
        unimportant_queue_.WaitAndPop(&msg);
        Unimportant_send(msg);
        nanosleep(&req,NULL);//
    //}//
    
  }
}
int Van::Important_send(Message& msg) {
  int send_bytes = SendMsg(msg);
  CHECK_NE(send_bytes, -1);
  //send_bytes_ += send_bytes;

  if (Postoffice::Get()->verbose() >= 2) {
    PS_VLOG(2) << msg.DebugString();
  }
  return send_bytes;
}
int Van::Unimportant_send(Message& msg) {
  int send_bytes = SendMsg_UDP(msg.meta.channel-1, msg, 0);
  CHECK_NE(send_bytes, -1);
  //send_bytes_ += send_bytes;

  if (Postoffice::Get()->verbose() >= 2) {
    PS_VLOG(2) << msg.DebugString();
  }
  return send_bytes;
}

int Van::Send( Message& msg, int channel, int tag) {
	int send_bytes = 0;
#ifdef ENCODE
    if(enable_encode && msg.meta.msg_type == 2){  //if msg is push's gradient,then encode the msg
        //std::cout << "***" << msg.DebugString() << std::endl;
        //msg_float_print(msg, 20);
        encode(msg);
        //std::cout << "###" << msg.DebugString() << std::endl;
    }
#endif
  if(channel == 0){
#ifdef ADAPTIVE_K
    /* if(msg.meta.push && msg.meta.request){
          if(msg.meta.first_key == 0) monitor_count = 0;
         /*  unsigned seed = time(NULL) + my_node_.id;
          float monitor_rate = 20; 
          if (rand_r(&seed) % 100 < monitor_rate) {
            msg.meta.udp_reliable=1;
          }*/
         /* if (monitor_count % 100 == 0) {
            msg.meta.udp_reliable=1;
          }
          if(msg.meta.udp_reliable){
              //std::cout << "ready to monitor this message" << std::endl;
              resender_->AddOutgoing(msg);
          }
          monitor_count++;
      } */ 
#endif
	  send_bytes = SendMsg(msg);

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
		  std::cout << "Van::Send, record outgoing msg" << std::endl;
          
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

void Van::Receiving_TCP() {
  Meta nodes;
  Meta recovery_nodes;  // store recovery nodes
  recovery_nodes.control.cmd = Control::ADD_NODE;

  while (true) {
    Message msg;
    int recv_bytes = RecvMsg_TCP(&msg);
    // dicide to drop the msg or not
    
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }
#ifdef ENCODE
    if(enable_encode && msg.meta.msg_type == 2){   //if msg is push's gradient,then decode it
      // std::cout << "$$$" << msg.DebugString() << std::endl;
       decode(msg);
       //std::cout << "@@@" << msg.DebugString() << std::endl;
       //msg_float_print(msg, 20);
    }
#endif
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
     /* if(msg.meta.udp_reliable){
        LOG(WARNING) <<"in tcp" <<msg.DebugString();
    } */
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
#ifdef ENCODE
    if(enable_encode && msg.meta.msg_type == 2){   //if msg is push's gradient,then decode it
       //std::cout << "$$$" << msg.DebugString() << std::endl;
       decode(msg);
       //std::cout << "@@@" << msg.DebugString() << std::endl;
       //msg_float_print(msg, 20);
    }
#endif
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
    //std::cout << msg.DebugString() << std::endl;
    /* if(msg.meta.udp_reliable){
        LOG(WARNING) << msg.DebugString();
    } */
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
      if(msg.meta.sender == 9)
        udp_recv++;
	  
    }
  }
}

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
    //if (resender_ && resender_->AddIncomming(msg)) continue;

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
	  if(msg.meta.sender == 9)
        tcp_recv++;//
    }
  }
}

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
  
  pb.set_seq_begin(meta.seq_begin);
  pb.set_seq_end(meta.seq_end);
  pb.set_seq(meta.seq);
  pb.set_udp_reliable(meta.udp_reliable);
  pb.set_channel(meta.channel);
  for (auto v : meta.compr) pb.add_compr(v);
  pb.set_msg_type(meta.msg_type);
  pb.set_push_op(meta.push_op_num);
  pb.set_val_bytes(meta.val_bytes);
  pb.set_total_bytes(meta.total_bytes);
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
  meta->seq = pb.seq();
  meta->seq_begin = pb.seq_begin();
  meta->seq_end = pb.seq_end();
  
  meta->udp_reliable = pb.udp_reliable();
  meta->channel = pb.channel();
  for(int i = 0; i < pb.compr_size();++i){
          meta->compr.push_back(pb.compr(i));
    }
  meta->msg_type = pb.msg_type();
  meta->push_op_num = pb.push_op();
  meta->val_bytes = pb.val_bytes();
  meta->total_bytes = pb.total_bytes();
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
