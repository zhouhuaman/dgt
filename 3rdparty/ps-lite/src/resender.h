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

/* #ifndef CHANNEL_MLR
#define CHANNEL_MLR
#endif */
/* #ifndef ADAPTIVE_K
#define ADAPTIVE_K
#endif */
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
#ifdef CHANNEL_MLR
    /**
   * \brief add an outgoining message
   *
   */
  void AddOutgoing(int channel, const Message& msg) {
	std::cout << "Enter AddOutgoing" << std::endl;
    if (msg.meta.control.cmd == Control::ACK) return;
    CHECK_NE(msg.meta.timestamp, Meta::kEmpty) << msg.DebugString();
    auto key = GetKey(msg);
	//std::cout << "key " << key << "have stored" << "at role = " << van_->my_node().role<< std::endl;
    //std::lock_guard<std::mutex> lk(mu_);
    // already buffered, which often due to call Send by the monitor thread
    std::cout << "Enter addoutgoing channel = " << channel << std::endl;
    if (send_buff_[channel].find(key) != send_buff_[channel].end()) return;

    auto& ent = send_buff_[channel][key];
	
    ent.msg = msg;
    ent.send = Now();
    ent.num_retry = 0;
    send_msg_cnt[channel]++;
  }
 /**
   * \brief update send_buff_ according to timestamp
   *
   */
  void Update_Sendbuff(int timestamp) {
    int cnt = 0;
    mu_.lock();
	for (auto it = send_buff_.begin(); it != send_buff_.end(); ++it) { 
      if(it->second.msg.meta.timestamp == timestamp){
            send_buff_.erase(it);
            cnt++;
        }
    }
    mu_.unlock();
    std::cout << timestamp << ",erase " << cnt << "message" << std::endl;
  }

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
      std::cout << "get an ACK, channel = " << msg.meta.channel << std::endl;
	  //std::cout << "key " << key << "have acked" << "at role = " << van_->my_node().role << std::endl;
      auto it = send_buff_[msg.meta.channel].find(key);
      if (it != send_buff_[msg.meta.channel].end()) send_buff_[msg.meta.channel].erase(it);
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
      ack.meta.channel = msg.meta.channel;
      ack.meta.control.cmd = Control::ACK;
      ack.meta.control.msg_sig = key;
      van_->Send(ack,0,0);
      // warning
      if (duplicated) LOG(WARNING) << "Duplicated message: " << msg.DebugString() << "key:" << key << "at role = " << van_->my_node().role << std::endl;
      return duplicated;
    }
  }
  
#else
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
	
    ent.msg = msg;
	
    ent.send = Now();
    ent.num_retry = 0;
    send_msg_cnt++;

  }
  /**
   * \brief add an incomming message
   * \brief return true if msg has been added before or a ACK message
   */
  bool AddIncomming(const Message& msg) {
    // a message can be received by multiple times
    if (msg.meta.control.cmd == Control::TERMINATE) {
      return false;
    } else if (msg.meta.control.cmd == Control::ACK) {
      mu_.lock();
      auto key = msg.meta.control.msg_sig;
	  //std::cout << "key " << key << "have acked" << "at role = " << van_->my_node().role << std::endl;
      auto it = send_buff_.find(key);
/* #ifdef ADAPTIVE_K
      Time now = Now();
      int meta_size= sizeof(it->second.msg.meta);
      int data_size=0;
      if (it->second.msg.data.size()) {
        for (const auto& d : it->second.msg.data)  data_size += d.size();
      }
      long transfer_time = now.count()-it->second.send.count();
      //std::cout << "meta_size = " << meta_size << " data_len= " << data_size << std::endl;   
      //std::cout << "transfer_time = " << transfer_time << " us" << std::endl;
      float tp = (meta_size+ data_size)*1000.0*1000.0/transfer_time;
      //std::cout << "throughput = " << tp << std::endl;
      throughput.push_back(tp);
#endif */
      if (it != send_buff_.end()) send_buff_.erase(it);
     // std::cout << "send_buff_size= " << send_buff_.size() << std::endl;
      
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
      van_->Send(ack,0,0);
      //std::cout<< "send an ack--" << ack.DebugString() << std::endl;
      // warning
      //if (duplicated) LOG(WARNING) << "Duplicated message: " << msg.DebugString() << "key:" << key << "at role = " << van_->my_node().role << std::endl;
      return duplicated;
    }
  }
#endif


 private:
  //using Time = std::chrono::milliseconds;
  using Time = std::chrono::microseconds;
  // the buffer entry
  struct Entry {
    Message msg;
    Time send;
    int num_retry = 0;
  };
#ifdef ADAPTIVE_K
    public:
    std::vector<float> throughput;
    float get_average_throughput(){
        //std::cout << "before accumulate, size = " << throughput.size() << std::endl;
        float sum = std::accumulate(std::begin(throughput),std::end(throughput),0.0);
        float avg = sum/throughput.size();
        //std::cout << "sum = " << sum << "size=" << throughput.size() << std::endl;
        throughput.clear();
        //std::cout << "avg = " << avg << std::endl;
        return avg;
    }
#endif
#ifdef CHANNEL_MLR
  public:
  std::vector<std::unordered_map<uint64_t, Entry>> send_buff_;
  std::vector<int> send_msg_cnt;
  int channel_num;
  std::vector<float> MLR;
  
  void init_channel_info(int ch_n, std::vector<float> mlr){
      std::cout << "van:147,ch_n = " << ch_n << std::endl;
      channel_num = 3;
      MLR.resize(ch_n);
      std::cout << "enter init_channel_info" << std::endl;
      std::copy(mlr.begin(),mlr.end(), MLR.begin());
      send_msg_cnt.resize(ch_n);
      send_buff_.resize(ch_n);
  }
  int get_channel_num(){
      return channel_num;
  }
  void reset_channel_info(int channel, float mlr){
      MLR[channel] = mlr;
      send_buff_[channel].clear();
      send_msg_cnt[channel] = 0;
  }
  float get_realtime_lr(int channel){
      std::cout << "[" <<channel << "]" << send_buff_[channel].size() << ":" << send_msg_cnt[channel] << std::endl;
      //return send_buff_[channel].size()/(float)send_msg_cnt[channel];
      return 1.0;
  }
#else
  std::unordered_map<uint64_t, Entry> send_buff_;
  uint64_t send_msg_cnt = 0;
#endif
  private:
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
#ifdef CHANNEL_MLR
 void Monitoring() {
    while (!exit_) {
      std::this_thread::sleep_for(Time(timeout_));
      std::vector<Message> resend;
      Time now = Now();
      mu_.lock();
      /* for(int i = 0; i < channel_num; ++i){
          for (auto& it : send_buff_[i]) {
                if (it.second.send + Time(timeout_) * (1+it.second.num_retry) < now && get_realtime_lr(i) > MLR[i]) {
                  /* if(it.second.msg.data.size() > 0){
                      char* p_key = it.second.msg.data[0].data();
                      if( *(uint64_t*)p_key!= it.second.msg.meta.first_key){
                          std::cout << "AT"<<it.first<<"?find key error,correct it" << (SArray<Key>)it.second.msg.data[0] <<"-->" << it.second.msg.meta.first_key << std::endl;
                          uint64_t key = (uint64_t)it.second.msg.meta.first_key;
                          memcpy(p_key, &key, sizeof(key));
                          
                          std::cout << "!correct completed, key = " << (SArray<Key>)it.second.msg.data[0] << std::endl;
                         
                    }
                    } */
                 // resend.push_back(it.second.msg);
                  //++it.second.num_retry;
                 /*  LOG(WARNING) << van_->my_node().ShortDebugString()
                               << ": Timeout to get the ACK message. Resend (retry="
                               << it.second.num_retry << ") " << it.second.msg.DebugString(); */
                  
                 // if(it.second.msg.data.size() > 0) std::cout<< "data.size = " << it.second.msg.data.size()<<" key=" << (SArray<Key>)it.second.msg.data[0] << "len = " << (SArray<int>)it.second.msg.data[2]<< std::endl;
                  //CHECK_LT(it.second.num_retry, max_num_retry_);
             /*   }
              }
              mu_.unlock();

              for (auto& msg : resend) van_->Send(msg,i+1,0);
              resend.clear();
            }*/
      } 
      
  }
#else
  void Monitoring() {
    while (!exit_) {
      std::this_thread::sleep_for(Time(timeout_));
      std::vector<Message> resend;
      Time now = Now();
      mu_.lock();
      
      for (auto& it : send_buff_) {
        if (it.second.send + Time(timeout_) * (1+it.second.num_retry) < now) {
		  /* if(it.second.msg.data.size() > 0){
			  char* p_key = it.second.msg.data[0].data();
			  if( *(uint64_t*)p_key!= it.second.msg.meta.first_key){
				  std::cout << "AT"<<it.first<<"?find key error,correct it" << (SArray<Key>)it.second.msg.data[0] <<"-->" << it.second.msg.meta.first_key << std::endl;
				  uint64_t key = (uint64_t)it.second.msg.meta.first_key;
				  memcpy(p_key, &key, sizeof(key));
				  
				  std::cout << "!correct completed, key = " << (SArray<Key>)it.second.msg.data[0] << std::endl;
				 
			}
			} */
          resend.push_back(it.second.msg);
          ++it.second.num_retry;
          //LOG(WARNING) << van_->my_node().ShortDebugString()
                      // << ": Timeout to get the ACK message. Resend (retry="
                      // << it.second.num_retry << ") " << it.second.msg.DebugString();
		  
		 // if(it.second.msg.data.size() > 0) std::cout<< "data.size = " << it.second.msg.data.size()<<" key=" << (SArray<Key>)it.second.msg.data[0] << "len = " << (SArray<int>)it.second.msg.data[2]<< std::endl;*/
          //CHECK_LT(it.second.num_retry, max_num_retry_);
        }
      }
      mu_.unlock();
      for (auto& msg : resend) van_->Send(msg,msg.meta.channel,0); 
    }
  }
#endif
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
