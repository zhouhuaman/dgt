/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_ZMQ_VAN_H_
#define PS_ZMQ_VAN_H_
#include <zmq.h>
#include <stdlib.h>
#include <thread>
#include <string>
#include "ps/internal/van.h"
#include <assert.h>
#include <stdlib.h>
#if _MSC_VER
#define rand_r(x) rand()
#endif

#ifndef CHANNEL_LOG
#define CHANNEL_LOG
#endif
namespace ps {
/**
 * \brief be smart on freeing recved data
 */
inline void FreeData(void *data, void *hint) {
  if (hint == NULL) {
    delete [] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}
#ifdef UDP_CHANNEL
inline void FreeData_malloc(void *data, void *hint) {
  if (hint == NULL) {
    free(static_cast<char*>(data));
  } else {
    free(static_cast<SArray<char>*>(hint));
  }
}
#endif
/**
 * \brief ZMQ based implementation
 */
class ZMQVan : public Van {
 public:
  ZMQVan() { }
  virtual ~ZMQVan() { }

 protected:
  void Start(int customer_id) override {
    // start zmq
    start_mu_.lock();
    if (context_ == nullptr) {
      context_ = zmq_ctx_new();
      CHECK(context_ != NULL) << "create 0mq context failed";
      zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
    }

    start_mu_.unlock();
    // zmq_ctx_set(context_, ZMQ_IO_THREADS, 4);
    Van::Start(customer_id);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();
    // close sockets
    int linger = 0;
    int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(receiver_), 0);
    for (auto& it : senders_) {
      int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
      CHECK(rc == 0 || errno == ETERM);
      CHECK_EQ(zmq_close(it.second), 0);
    }
    senders_.clear();
    zmq_ctx_destroy(context_);
    context_ = nullptr;

  }
#ifdef DOUBLE_CHANNEL
std::vector<int> Bind_UDP(const Node& node, int max_retry) override {
    std::vector<int> tmp_udp_port;
    for(int i = 0; i < node.udp_port.size(); ++i){
        udp_receiver_ = zmq_socket(context_, ZMQ_DISH);
        CHECK(udp_receiver_ != NULL)
    << "create udp_receiver[" << i<<" ]socket failed: " << zmq_strerror(errno);
        /* int udp_recv_buf_size = 4096*1024;  //4M 
        int rc = zmq_setsockopt(udp_receiver_, ZMQ_RCVBUF, &udp_recv_buf_size, sizeof(udp_recv_buf_size));
        assert(rc == 0);
        int check_rcv_buff = 0;
        size_t check_len = sizeof(check_rcv_buff);
        rc = zmq_getsockopt(udp_receiver_, ZMQ_RCVBUF, &check_rcv_buff, &check_len);
        
        std::cout << "recv["<< i <<"] buf size = " << check_rcv_buff << std::endl; */
        int rc;
        int local = GetEnv("DMLC_LOCAL", 0);
        std::string hostname = node.hostname.empty() ? "*" : node.hostname;
        int use_kubernetes = GetEnv("DMLC_USE_KUBERNETES", 0);
        if (use_kubernetes > 0 && node.role == Node::SCHEDULER) {
          hostname = "0.0.0.0";
        }
        std::string udp_addr = local ? "ipc:///tmp/" : "udp://" + hostname + ":";
        
        int udp_port = node.udp_port[i];
        unsigned seed = static_cast<unsigned>(time(NULL)+udp_port);
        for (int i = 0; i < max_retry+1; ++i) {
          auto address = udp_addr + std::to_string(udp_port);
          if (zmq_bind(udp_receiver_, address.c_str()) == 0) break;
          if (i == max_retry) {
            udp_port = -1;
          } else {
            udp_port = 10000 + rand_r(&seed) % 40000;
          }
        }
        rc = zmq_join(udp_receiver_, "GRADIENT");
        assert(rc == 0);
        std::cout << "Bind Udp channel["<< i+1 <<"] SUCCESS!!"<< std::endl;
        udp_receiver_vec.push_back(udp_receiver_);
        tmp_udp_port.push_back(udp_port);
    }
    return tmp_udp_port;

  }
  
  void Connect_UDP(const Node& node) override {

    CHECK_NE(node.id, node.kEmpty);
    //CHECK_NE(node.udp_port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    auto it = udp_senders_.find(id);
    if (it != udp_senders_.end()) {
      for(int i = 0; i < it->second.size(); ++i){
          zmq_close(it->second[i]);
      }
      
    }
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }
    std::cout << "udp_port.size =" <<  node.udp_port.size() << std::endl;
    for(int i = 0; i < node.udp_port.size(); ++i){
        std::cout << node.udp_port[i] << std::endl;
    }
    for(int i = 0; i < node.udp_port.size(); ++i){
        void *udp_sender = zmq_socket(context_, ZMQ_RADIO);
        
        CHECK(udp_sender != NULL)
            << zmq_strerror(errno)
            << ". it often can be solved by \"sudo ulimit -n 65536\""
            << " or edit /etc/security/limits.conf";
        if (my_node_.id != Node::kEmpty) {
          std::string my_id = "ps" + std::to_string(my_node_.id);
          zmq_setsockopt(udp_sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
          int tos = (node.udp_port.size()-i-1)*32;
          if(zmq_setsockopt(udp_sender, ZMQ_TOS, &tos, sizeof(tos))==0){
              int dscp = (node.udp_port.size()-i-1)*8;
              std::string passwd = CHECK_NOTNULL(Environment::Get()->find("SUDO_PASSWD"));
              std::string command = "echo " + passwd + " | " +"sudo -S iptables -t mangle -A OUTPUT -p udp --dst " + node.hostname + " --dport "+ std::to_string(node.udp_port[i])+" -j DSCP --set-dscp "+std::to_string(dscp);
              std::cout << "command = " << command << std::endl;
              system(command.c_str());
              std::cout << "Successful to Set " << "udp[" << i+1 << "]:"<< my_node_.id << "=>" << node.id << "(" << node.hostname.c_str() << ":" << node.udp_port[i] << "):" << "tos=" << tos << std::endl;
              
          }else{
              std::cout << "Fail to Set " << "udp[" << i+1 << "]:"<< my_node_.id << "=>" << node.id << "(" << node.hostname.c_str() << ":" << node.udp_port[i] << "):" << "tos=" << tos << std::endl;
          }
        }
       
        /* int udp_send_buf_size = 4096*1024;  //4M 
        int rc = zmq_setsockopt(udp_sender, ZMQ_SNDBUF, &udp_send_buf_size, sizeof(udp_send_buf_size));
        assert(rc == 0);
        int check_snd_buff = 0;
        size_t check_len = sizeof(check_snd_buff);
        rc = zmq_getsockopt(udp_sender, ZMQ_SNDBUF, &check_snd_buff, &check_len);
        std::cout << "send buf size = " << check_snd_buff << std::endl; */
        int rc;
        // connect
        std::string addr = "udp://" + node.hostname + ":" + std::to_string(node.udp_port[i]);
        
        if (GetEnv("DMLC_LOCAL", 0)) {
          addr = "ipc:///tmp/" + std::to_string(node.udp_port[i]);
        }
        if (zmq_connect(udp_sender, addr.c_str()) != 0) {
          PS_VLOG(1) <<  "UDP[channel "<< i+1 <<"]:connect to " + addr + " failed: " + zmq_strerror(errno);
        }else{
          PS_VLOG(1) <<  "UDP[channel "<< i+1 <<"]:connect to " + addr + " success!!! ";
        }
        //udp_senders_[id] = udp_sender;
        udp_senders_[id].push_back(udp_sender);
        //write server info to log
    #ifdef CHANNEL_LOG
        if(node.role == 0){
            /* if(i == 0){
                 std::string file_str = "/tmp/channel"+ std::to_string(my_node_.id)+ ".csv";
                fp = fopen(file_str.c_str(),"w+");
                if(!fp){
                    std::cout << "failed to open"<< file_str << std::endl;
                }else{
                    std::cout << "success to open" << file_str << std::endl;
                }
            } */
            fprintf(fp,"%d,%d,%d,%s,%s,%d,%d\n",my_node_.id, node.id, i+1, "udp", node.hostname.c_str(), node.udp_port[i], (i+1)*8);
            if(i == node.udp_port.size()-1){
                fflush(fp);
                fclose(fp);
            }
        }
        //fprintf(fp,"%d,%d,%d,%s,%d,%d\n",my_node_.id, node.id, 1, node.hostname.c_str(), node.udp_port, 1);
        //fflush(fp);
    #endif
        std::cout << "UDP: Connect  to" << node.DebugString()<<" SUCCESS!!" << std::endl;
    }
	
  }
#endif
  int Bind(const Node& node, int max_retry) override {
    receiver_ = zmq_socket(context_, ZMQ_ROUTER);
    CHECK(receiver_ != NULL)
        << "create receiver socket failed: " << zmq_strerror(errno);
    int local = GetEnv("DMLC_LOCAL", 0);
    std::string hostname = node.hostname.empty() ? "*" : node.hostname;
    int use_kubernetes = GetEnv("DMLC_USE_KUBERNETES", 0);
    if (use_kubernetes > 0 && node.role == Node::SCHEDULER) {
      hostname = "0.0.0.0";
    }
    std::string addr = local ? "ipc:///tmp/" : "tcp://" + hostname + ":";
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL)+port);
    for (int i = 0; i < max_retry+1; ++i) {
      auto address = addr + std::to_string(port);
      if (zmq_bind(receiver_, address.c_str()) == 0) break;
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    std::cout << "TCP:Bind SUCCESS!! port = "<< port << std::endl;
    return port;
  }

  void Connect(const Node& node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    auto it = senders_.find(id);
    if (it != senders_.end()) {
      zmq_close(it->second);
    }
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }
/* #ifdef UDP_CHANNEL
	void *sender = zmq_socket(context_, ZMQ_RADIO);
#else */
    void *sender = zmq_socket(context_, ZMQ_DEALER);
//#endif
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
    if (my_node_.id != Node::kEmpty) {
      std::string my_id = "ps" + std::to_string(my_node_.id);
      zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
      if(node.udp_port.size()!=0){
          int tos = node.udp_port.size()*32;
          if(zmq_setsockopt(sender, ZMQ_TOS, &tos, sizeof(tos))==0){
              std::cout << "Successful to Set " << "tcp[" << 0 << "]:"<< my_node_.id << "=>" << node.id << "(" << node.hostname.c_str() << ":" << node.port << "):" << "tos=" << tos << std::endl;
              
          }else{
              std::cout << "Fail to Set " << "tcp[" << 0 << "]:"<< my_node_.id << "=>" << node.id << "(" << node.hostname.c_str() << ":" << node.port << "):" << "tos=" << tos << std::endl;
          }
      }
      
    }
    // connect
/* #ifdef UDP_CHANNEL
	std::string addr = "udp://" + node.hostname + ":" + std::to_string(node.port);
#else */
    std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
//#endif
    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) <<  "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    senders_[id] = sender;
#ifdef CHANNEL_LOG
    if(node.role ==0){
        std::string file_str = "/tmp/channel"+ std::to_string(my_node_.id)+ ".csv";
        fp = fopen(file_str.c_str(),"w+");
        if(!fp){
            std::cout << "failed to open"<< file_str << std::endl;
        }else{
            std::cout << "success to open" << file_str << std::endl;
        }
        fprintf(fp,"%d,%d,%d,%s,%s,%d,%d\n",my_node_.id, node.id,0, "tcp", node.hostname.c_str(), node.port, 0);
   
    }
#endif
	std::cout << "TCP:Connect  to" << node.DebugString()<<"  SUCCESS!!" << std::endl;
  }
  
/*if DOUBLE_CHANNEL, SendMsg=> SendMsg_TCP and SendMsg_UDP*/
#ifdef DOUBLE_CHANNEL
  int SendMsg_TCP(Message& msg, int tag) override {
    std::lock_guard<std::mutex> lk(mu_);
    // find the socket
	//static unsigned int count = 0;
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "tcp:there is no socket to node " << id;
      return -1;
    }
    void *socket = it->second;
	//std::cout << "#160:SendMsg:" << std::endl;
	int meta_size; char* meta_buf;
	int n = msg.data.size();
    PackMeta(msg.meta, &meta_buf, &meta_size);
	size_t tot_bytes = 0;
	size_t addr_offset = 0;
	int send_bytes = 0;
	tot_bytes += sizeof(meta_size);
	tot_bytes += meta_size;
	for(int i = 0; i < n; ++i){
		tot_bytes += msg.data[i].size();
	}

	char *send_buf = (char*) malloc(tot_bytes);
	
	memcpy(send_buf, (char*)&meta_size, sizeof(meta_size));
	//std::cout << "#172:SendMsg" << std::endl;
	addr_offset += sizeof(meta_size);
	memcpy(send_buf+addr_offset, meta_buf, meta_size);
	//std::cout << "#175:SendMsg" << std::endl;
	addr_offset += meta_size;
	for(int i = 0; i < n; ++i){
		//SArray<char>* data = new SArray<char>(msg.data[i]);
		memcpy(send_buf+addr_offset, msg.data[i].data(), msg.data[i].size());
		addr_offset += msg.data[i].size();
		//delete data;
	}
	assert(tot_bytes == addr_offset);
	zmq_msg_t data_msg;
	zmq_msg_init_data(&data_msg, send_buf, tot_bytes, FreeData_malloc, NULL);
	//std::cout << "#187:SendMsg" << std::endl;
	while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == tot_bytes) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "tcp:failed to send message to node [" << id
                     << "] errno: " << errno << " " << zmq_strerror(errno);
        return -1;
      }
	//free(send_buf);
	send_bytes = tot_bytes;
    return send_bytes;
  }
  
  int SendMsg_UDP(int channel, Message& msg, int tag) override {
    std::lock_guard<std::mutex> lk(mu_);
    // find the socket
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    auto it = udp_senders_.find(id);
    if (it == udp_senders_.end()) {
      LOG(WARNING) << "udp:there is no socket to node " << id;
      return -1;
    }
    void *socket = it->second[channel];
	//std::cout << "#160:SendMsg:" << std::endl;
	int meta_size; char* meta_buf;
	int n = msg.data.size();

    PackMeta(msg.meta, &meta_buf, &meta_size);
	
	size_t tot_bytes = 0;
	size_t addr_offset = 0;
	int send_bytes = 0;
	tot_bytes += sizeof(meta_size);
	tot_bytes += meta_size;
	for(int i = 0; i < n; ++i){
		tot_bytes += msg.data[i].size();
	}
	char *send_buf = (char*) malloc(tot_bytes);
	
	memcpy(send_buf, (char*)&meta_size, sizeof(meta_size));
	addr_offset += sizeof(meta_size);
	memcpy(send_buf+addr_offset, meta_buf, meta_size);
	addr_offset += meta_size;
	for(int i = 0; i < n; ++i){
		//SArray<char>* data = new SArray<char>(msg.data[i]);
		
		memcpy(send_buf+addr_offset, msg.data[i].data(), msg.data[i].size());
		addr_offset += msg.data[i].size();
		//delete data;
	}
	assert(tot_bytes == addr_offset);
	zmq_msg_t data_msg;
	zmq_msg_init_data(&data_msg, send_buf, tot_bytes, FreeData_malloc, NULL);
	zmq_msg_set_group (&data_msg, "GRADIENT");
	while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == tot_bytes) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "udp:failed to send message to node [" << id
                     << "] errno: " << errno << " " << zmq_strerror(errno);
        return -1;
      }
    //free(send_buf);
	send_bytes = tot_bytes;
    return send_bytes;
  }
#else
/*ole version SendMsg*/
int SendMsg(Message& msg) override {
    std::lock_guard<std::mutex> lk(mu_);
    // find the socket
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "udp:there is no socket to node " << id;
      return -1;
    }
    void *socket = it->second;
    // send meta
    int meta_size; char* meta_buf;
	
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int tag = ZMQ_SNDMORE;
    int n = msg.data.size();
    if (n == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      return -1;
    }
    // zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;
    // send data
    for (int i = 0; i < n; ++i) {
      zmq_msg_t data_msg;
      SArray<char>* data = new SArray<char>(msg.data[i]);
      int data_size = data->size();
      zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
      if (i == n - 1) tag = 0;
      while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to send message to node [" << id
                     << "] errno: " << errno << " " << zmq_strerror(errno)
                     << ". " << i << "/" << n;
        return -1;
      }
      // zmq_msg_close(&data_msg);
      send_bytes += data_size;
    }
    return send_bytes;
  }
#endif

/*if DOUBLE_CHANNEL, RecvMSG=> RecvMSG_TCP and RecvMSG_UDP*/
#ifdef DOUBLE_CHANNEL
  int RecvMsg_UDP(int channel, Message* msg) override {
    msg->data.clear();
    size_t recv_bytes = 0;
    for (int i = 0; ; ++i) {
      zmq_msg_t* zmsg = new zmq_msg_t;
      CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
      while (true) {
        if (zmq_msg_recv(zmsg, udp_receiver_vec[channel], 0) != -1) break;
        if (errno == EINTR) {
          std::cout << "interrupted";
          continue;
        }
        LOG(WARNING) << "failed to receive message. errno: "
                     << errno << " " << zmq_strerror(errno);
        return -1;
      }
      char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;

	  int  meta_size;
	  int addr_offset = 0;
	  memcpy((void*)&meta_size, buf+addr_offset, sizeof(meta_size));
	  addr_offset += sizeof(meta_size);
	  
	  // task
      UnpackMeta(buf+addr_offset, meta_size, &(msg->meta));
	  //decide drop the msg or not*****************************************
    if(my_node_.role == 0 && msg->meta.msg_type == 2){  //server
       if(msg->meta.push_op_num < channel_manage_sheet[msg->meta.sender][msg->meta.first_key]){
           continue;
       }
    }
	  //*************************************************************
	 addr_offset += meta_size;
	
	 if(msg->meta.keys_len > 0){
		   SArray<char> data;
		   
		   data.reset(buf+addr_offset, msg->meta.keys_len, [zmsg, size](char* buf) {
            
		   });
		   msg->data.push_back(data);
		   addr_offset += msg->meta.keys_len;
		   if(msg->meta.lens_len > 0){
				data.reset(buf+addr_offset, msg->meta.vals_len, [zmsg, size](char* buf) {
            
				});
				msg->data.push_back(data);
				addr_offset += msg->meta.vals_len;
				data.reset(buf+addr_offset, msg->meta.lens_len, [zmsg, size](char* buf) {
					/* zmq_msg_close(zmsg);
					delete zmsg; */
				});
				msg->data.push_back(data);
				addr_offset += msg->meta.lens_len;
			}else{
				data.reset(buf+addr_offset, msg->meta.vals_len, [zmsg, size](char* buf) {
					/* zmq_msg_close(zmsg);
					delete zmsg; */
					});
				msg->data.push_back(data);
				addr_offset += msg->meta.vals_len;
			}	   
	    }	
	  zmq_msg_close(zmsg);
      delete zmsg;
	  break;
    }
    
    return recv_bytes;
  }
  
/*define RecvMSG_TCP*/
int RecvMsg_TCP(Message* msg) override {
    msg->data.clear();
	//static unsigned int count = 0;
    size_t recv_bytes = 0;
    for (int i = 0; ; ++i) {
      zmq_msg_t* zmsg = new zmq_msg_t;
      CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
      while (true) {
        if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
        if (errno == EINTR) {
          std::cout << "interrupted";
          continue;
        }
        LOG(WARNING) << "failed to receive message. errno: "
                     << errno << " " << zmq_strerror(errno);
        return -1;
      }
      char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;
	  if(!identify_flag){
		  identify_flag = true;
		  zmq_msg_close(zmsg);
		  delete zmsg;
		  continue;
	  }
	  
	  int  meta_size;
	  int addr_offset = 0;
	  memcpy((void*)&meta_size, buf+addr_offset, sizeof(meta_size));
	  addr_offset += sizeof(meta_size);
	  // task
      UnpackMeta(buf+addr_offset, meta_size, &(msg->meta));
      
	  //*******************************************************channel manage
      if(my_node_.role == 0 && msg->meta.msg_type == 1){  //server)  //init channel manage
            channel_manage_sheet[msg->meta.sender][msg->meta.first_key] = msg->meta.push_op_num+1;  //init expect push_op_num
        }
      if(my_node_.role == 0 && msg->meta.msg_type == 2){  //server
       
            if(msg->meta.seq == msg->meta.seq_end){
                channel_manage_sheet[msg->meta.sender][msg->meta.first_key] += 1;   //expect next push_op
            }
        }
    //*******************************************************************************
	  
	 addr_offset += meta_size;

	 if(msg->meta.keys_len > 0){
		   SArray<char> data;
		   
		   data.reset(buf+addr_offset, msg->meta.keys_len, [zmsg, size](char* buf) {
            
		   });
		   msg->data.push_back(data);
		   addr_offset += msg->meta.keys_len;
		   if(msg->meta.lens_len > 0){
				data.reset(buf+addr_offset, msg->meta.vals_len, [zmsg, size](char* buf) {
            
				});
				msg->data.push_back(data);
				addr_offset += msg->meta.vals_len;
				data.reset(buf+addr_offset, msg->meta.lens_len, [zmsg, size](char* buf) {
					/* zmq_msg_close(zmsg);
					delete zmsg; */
				});
				msg->data.push_back(data);
				addr_offset += msg->meta.lens_len;
			}else{
				data.reset(buf+addr_offset, msg->meta.vals_len, [zmsg, size](char* buf) {
					/* zmq_msg_close(zmsg);
					delete zmsg; *///
					});
				msg->data.push_back(data);
				addr_offset += msg->meta.vals_len;
			}	   
	    }
		if (!zmq_msg_more(zmsg)) { identify_flag = false; }
        zmq_msg_close(zmsg);
        delete zmsg;
        break;
    }
    
    return recv_bytes;
  }
  
#else
/*old version RecvMsg*/
int RecvMsg(Message* msg) override {
    msg->data.clear();
	//static unsigned int count = 0;
    size_t recv_bytes = 0;
    for (int i = 0; ; ++i) {
      zmq_msg_t* zmsg = new zmq_msg_t;
      CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
      while (true) {
        if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
        if (errno == EINTR) {
          std::cout << "interrupted";
          continue;
        }
        LOG(WARNING) << "failed to receive message. errno: "
                     << errno << " " << zmq_strerror(errno);
        return -1;
      }
      char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;
      if (i == 0) {
        // identify
        msg->meta.sender = GetNodeID(buf, size);
        msg->meta.recver = my_node_.id;
        CHECK(zmq_msg_more(zmsg));
        zmq_msg_close(zmsg);
        delete zmsg;
      } else if (i == 1) {
        // task
        UnpackMeta(buf, size, &(msg->meta));
		//std::cout << msg->meta.DebugString();
        zmq_msg_close(zmsg);
        bool more = zmq_msg_more(zmsg);
        delete zmsg;
        if (!more) break;
      } else {
        // zero-copy
        SArray<char> data;
        data.reset(buf, size, [zmsg, size](char* buf) {
            zmq_msg_close(zmsg);
            delete zmsg;
          });
        msg->data.push_back(data);
        if (!zmq_msg_more(zmsg)) { break; }
      }
    }
    return recv_bytes;
  }
#endif
 private:
  /**
   * return the node id given the received identity
   * \return -1 if not find
   */
  int GetNodeID(const char* buf, size_t size) {
    if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
      int id = 0;
      size_t i = 2;
      for (; i < size; ++i) {
        if (buf[i] >= '0' && buf[i] <= '9') {
          id = id * 10 + buf[i] - '0';
        } else {
          break;
        }
      }
      if (i == size) return id;
    }
    return Meta::kEmpty;
  }

  void *context_ = nullptr;
  /**
   * \brief node_id to the socket for sending data to this node
   */
  std::unordered_map<int, void*> senders_;
#ifdef DOUBLE_CHANNEL
  std::unordered_map<int, std::vector<void*>> udp_senders_;
  std::vector<void *> udp_receiver_vec;;
  void *udp_receiver_ = nullptr;
  bool identify_flag = false; //if or not read the identify already
  std::unordered_map<int,std::unordered_map<int,int>> channel_manage_sheet;
#endif
#ifdef CHANNEL_LOG
  FILE *fp;
#endif
  std::mutex mu_;
  void *receiver_ = nullptr;
};
}  // namespace ps

#endif  // PS_ZMQ_VAN_H_





// monitors the liveness other nodes if this is
// a schedule node, or monitors the liveness of the scheduler otherwise
// aliveness monitor
// CHECK(!zmq_socket_monitor(
//     senders_[kScheduler], "inproc://monitor", ZMQ_EVENT_ALL));
// monitor_thread_ = std::unique_ptr<std::thread>(
//     new std::thread(&Van::Monitoring, this));
// monitor_thread_->detach();

// void Van::Monitoring() {
//   void *s = CHECK_NOTNULL(zmq_socket(context_, ZMQ_PAIR));
//   CHECK(!zmq_connect(s, "inproc://monitor"));
//   while (true) {
//     //  First frame in message contains event number and value
//     zmq_msg_t msg;
//     zmq_msg_init(&msg);
//     if (zmq_msg_recv(&msg, s, 0) == -1) {
//       if (errno == EINTR) continue;
//       break;
//     }
//     uint8_t *data = static_cast<uint8_t*>(zmq_msg_data(&msg));
//     int event = *reinterpret_cast<uint16_t*>(data);
//     // int value = *(uint32_t *)(data + 2);

//     // Second frame in message contains event address. it's just the router's
//     // address. no help

//     if (event == ZMQ_EVENT_DISCONNECTED) {
//       if (!is_scheduler_) {
//         PS_VLOG(1) << my_node_.ShortDebugString() << ": scheduler is dead. exit.";
//         exit(-1);
//       }
//     }
//     if (event == ZMQ_EVENT_MONITOR_STOPPED) {
//       break;
//     }
//   }
//   zmq_close(s);
// }
