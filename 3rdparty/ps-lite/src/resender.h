/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_RESENDER_H_
#define PS_RESENDER_H_
#include <chrono>
#include <vector>
#include <unordered_set>
#include <unordered_map>

#ifndef UDP_CHANNEL
#define UDP_CHANNEL
#endif
namespace ps {

/**
 * \brief resend a messsage if no ack is received within a given time
 */
class Resender {
 public:
  /**
   * \param timeout timeout in millisecond
   */
  Resender(int timeout, int max_num_retry, Van* van) {
    timeout_ = timeout;
    max_num_retry_ = max_num_retry;
    van_ = van;
	
	global_key = 0;
    monitor_ = new std::thread(&Resender::Monitoring, this);
	
	
  }
  ~Resender() {
    exit_ = true;
    monitor_->join();
    delete monitor_;
  }

  /**
   * \brief add an outgoining message
   *
   */
  void AddOutgoing(const Message& msg) {
	//std::cout << "Enter AddOutgoing" << std::endl;
    if (msg.meta.control.cmd == Control::ACK) return;
    CHECK_NE(msg.meta.timestamp, Meta::kEmpty) << msg.DebugString();
    auto key = GetKey(msg);
	//std::cout << "key " << key << "have stored" << "at role = " << van_->my_node().role<< std::endl;
    std::lock_guard<std::mutex> lk(mu_);
    // already buffered, which often due to call Send by the monitor thread
    if (send_buff_.find(key) != send_buff_.end()) return;

    auto& ent = send_buff_[key];
	/* if(msg.data.size() > 0){
		  if(((SArray<Key>)msg.data[0])[0] > 100000){
				std::cout << "resender->AddOutgoing#53:find error key" << (SArray<Key>)msg.data[0] << msg.meta.DebugString() << std::endl;
		   }
	} */
    ent.msg = msg;
	/* if(ent.msg.data.size() > 0){
		  if(((SArray<Key>)ent.msg.data[0])[0] > 100000){
				std::cout << "resender->AddOutgoing#59:ent.msg:find error key" << (SArray<Key>)ent.msg.data[0] << ent.msg.meta.DebugString() << std::endl;
		   }
	} */
	 /* if(ent.msg.data.size() > 0){
			  char* p_key = ent.msg.data[0].data();
			  //if( *(uint64_t*)p_key!= ent.msg.meta.first_key){
				  if(ent.msg.meta.recver == 9){
				  std::cout << "AT"<<key << " compare it" << *(uint64_t*)p_key <<"<-->" << ent.msg.meta.first_key << std::endl;
				  char *p_val = ent.msg.data[1].data();
				  std::cout << "AT "<<key << "val(first 8Bytes) = "<< *(uint64_t*)p_val <<",val(800-808Bytes) = "<< *(uint64_t*)(p_val+800)<< std::endl;
				  std::cout << ent.msg.DebugString() << std::endl;
				  }
			 // }
			   
			  
		  } */
    ent.send = Now();
    ent.num_retry = 0;
	
	
	 /* for (auto& it : send_buff_) {
		 if(it.second.msg.meta.data_num > 0){
		  if(((SArray<Key>)it.second.msg.data[0])[0] > 100000){
				std::cout << "resender->AddOutgoing#67:find error key" << (SArray<Key>)it.second.msg.data[0] << it.second.msg.meta.DebugString() << std::endl;
		   } 
	}
	 }*/
  }
#ifdef DOUBLE_CHANNEL
  bool AddIncomming_Push(const Message& msg){
	   // a message can be received by multiple times
	//std::cout << "Enter AddIncomming" << std::endl;
	
    if (msg.meta.control.cmd == Control::TERMINATE) {
      return false;
    } else if (msg.meta.control.cmd == Control::ACK) {
      mu_.lock();
      auto key = msg.meta.control.msg_sig;
	  //std::cout << "key " << key << "have acked" << "at role = " << van_->my_node().role << std::endl;
      auto it = send_buff_.find(key);
      if (it != send_buff_.end()) send_buff_.erase(it);
      mu_.unlock();
      return true;
    } else {
	  mu_.lock();
	  auto it = push_rcv_buff_.find(msg.meta.sender);
      if (it == push_rcv_buff_.end()){
		  std::vector<Push_Entry> tmp_vec;
		  tmp_vec.resize(msg.meta.key_end-msg.meta.key_begin+1);
		  push_rcv_buff_.insert(std::make_pair(msg.meta.sender, tmp_vec));
		  std::cout << "construct push_rcv_buff_" << std::endl;
		  /* push_rcv_buff_[msg.meta.sender] = std::vector<Push_Entry>;
		  push_rcv_buff_[msg.meta.sender].resize(msg.meta.key_end-msg.meta.key_begin+1); */
	  }
	  std::cout << msg.DebugString() << std::endl;
	  push_rcv_buff_[msg.meta.sender][msg.meta.first_key].incomming_msg_cnt += 1;
	  //push_rcv_buff_[msg.meta.sender][msg.meta.first_key].pre_msg = msg;
	  push_rcv_buff_[msg.meta.sender][msg.meta.first_key].pre_msg_seq += 1;
	  mu_.unlock();
	  return false;
	}
  }
#endif
  /**
   * \brief add an incomming message
   * \brief return true if msg has been added before or a ACK message
   */
  bool AddIncomming(const Message& msg) {
    // a message can be received by multiple times
	//std::cout << "Enter AddIncomming" << std::endl;
    if (msg.meta.control.cmd == Control::TERMINATE) {
      return false;
    } else if (msg.meta.control.cmd == Control::ACK) {
      mu_.lock();
      auto key = msg.meta.control.msg_sig;
	  //std::cout << "key " << key << "have acked" << "at role = " << van_->my_node().role << std::endl;
      auto it = send_buff_.find(key);
      if (it != send_buff_.end()) send_buff_.erase(it);
      mu_.unlock();
      return true;
    } else {
      mu_.lock();
      auto key = GetKey(msg);
      auto it = acked_.find(key);
      bool duplicated = it != acked_.end();
      if (!duplicated) acked_.insert(key);
	  //std::cout << "key " << key << "(no ack) have received, process,duplicated = " << duplicated << "at role = " << van_->my_node().role << msg.DebugString() << std::endl;
	  //if(!duplicated) std::cout << msg.DebugString();
      mu_.unlock();
      // send back ack message (even if it is duplicated)
      Message ack;
      ack.meta.recver = msg.meta.sender;
      ack.meta.sender = msg.meta.recver;
      ack.meta.control.cmd = Control::ACK;
      ack.meta.control.msg_sig = key;
      van_->Send(ack,1,0);
      // warning
      if (duplicated) LOG(WARNING) << "Duplicated message: " << msg.DebugString() << "key:" << key << "at role = " << van_->my_node().role << std::endl;
      return duplicated;
    }
  }
 #ifdef DOUBLE_CHANNEL
 struct Push_Entry{
	int incomming_msg_cnt = 0;
    int pre_msg_seq = 0;	
	Message pre_msg;
 };
  /*int -> record which sender;Push_Entry ->record the sender's push_msg history status*/
  std::unordered_map<int, std::vector<Push_Entry>> push_rcv_buff_; //
 #endif
 private:
  using Time = std::chrono::milliseconds;
  // the buffer entry
  struct Entry {
    Message msg;
    Time send;
    int num_retry = 0;
  };

  std::unordered_map<uint64_t, Entry> send_buff_;
  uint64_t global_key;
  
  uint64_t GetKey(const Message& msg) {
    CHECK_NE(msg.meta.timestamp, Meta::kEmpty) << msg.DebugString();
    uint16_t id = msg.meta.app_id;
    uint8_t sender = msg.meta.sender == Node::kEmpty ?
                     van_->my_node().id : msg.meta.sender;
    uint8_t recver = msg.meta.recver;
	
	//std::cout << id << ":" << sender << ":" << recver << ":" << msg.meta.timestamp << ":" << msg.meta.request << std::endl;
#ifdef UDP_CHANNEL
    //uint16_t first_key = msg.meta.first_key;
    if(msg.meta.control.cmd == Control::EMPTY){
		id = (uint16_t)msg.meta.first_key;
		//std::cout << "id=" << id << std::endl;
		return (static_cast<uint64_t>(id) << 48) |
			(static_cast<uint64_t>(sender) << 40) |
			(static_cast<uint64_t>(recver) << 32) |
			(msg.meta.timestamp << 1) | msg.meta.request;
	}else{
		return (static_cast<uint64_t>(id) << 48) |
        (static_cast<uint64_t>(sender) << 40) |
        (static_cast<uint64_t>(recver) << 32) |
        (msg.meta.timestamp << 1) | msg.meta.request;
	}
#else
    return (static_cast<uint64_t>(id) << 48) |
        (static_cast<uint64_t>(sender) << 40) |
        (static_cast<uint64_t>(recver) << 32) |
        (msg.meta.timestamp << 1) | msg.meta.request;
#endif
  }
  Time Now() {
    return std::chrono::duration_cast<Time>(
        std::chrono::high_resolution_clock::now().time_since_epoch());
  }

  void Monitoring() {
    while (!exit_) {
      std::this_thread::sleep_for(Time(timeout_));
      std::vector<Message> resend;
      Time now = Now();
      mu_.lock();
      for (auto& it : send_buff_) {
        if (it.second.send + Time(timeout_) * (1+it.second.num_retry) < now) {
		  if(it.second.msg.data.size() > 0){
			  char* p_key = it.second.msg.data[0].data();
			  if( *(uint64_t*)p_key!= it.second.msg.meta.first_key){
				  std::cout << "AT"<<it.first<<"?find key error,correct it" << (SArray<Key>)it.second.msg.data[0] <<"-->" << it.second.msg.meta.first_key << std::endl;
				  uint64_t key = (uint64_t)it.second.msg.meta.first_key;
				  memcpy(p_key, &key, sizeof(key));
				  
				  std::cout << "!correct completed, key = " << (SArray<Key>)it.second.msg.data[0] << std::endl;
				  //global_key = it.first;
				  
					//auto&env = send_buff_[it.first];
			  //if( *(uint64_t*)p_key!= it.msg.meta.first_key){
					/* std::cout << "AT " << it.first << "compare through key index" << *(uint64_t*)p_key <<"<-->" << env.msg.meta.first_key<< std::endl;
		  //}       
					 char *p_val = env.msg.data[1].data();
					 std::cout << "AT "<<it.first << "val(first 8Bytes) = "<< *(uint64_t*)p_val << ",val(800-808Bytes) = "<< *(uint64_t*)(p_val+800) << std::endl;
					 std::cout << env.msg.DebugString() << std::endl; */
			}
			}
			  
			  
		  
          resend.push_back(it.second.msg);
          ++it.second.num_retry;
         /*  LOG(WARNING) << van_->my_node().ShortDebugString()
                       << ": Timeout to get the ACK message. Resend (retry="
                       << it.second.num_retry << ") " << it.second.msg.DebugString(); */
		  
		 // if(it.second.msg.data.size() > 0) std::cout<< "data.size = " << it.second.msg.data.size()<<" key=" << (SArray<Key>)it.second.msg.data[0] << "len = " << (SArray<int>)it.second.msg.data[2]<< std::endl;
          //CHECK_LT(it.second.num_retry, max_num_retry_);
        }
      }
      mu_.unlock();

      for (auto& msg : resend) van_->Send(msg,1,0);
    }
  }
  std::thread* monitor_;
  std::unordered_set<uint64_t> acked_;
  std::atomic<bool> exit_{false};
  std::mutex mu_;
  int timeout_;
  int max_num_retry_;
  Van* van_;
};
}  // namespace ps
#endif  // PS_RESENDER_H_
