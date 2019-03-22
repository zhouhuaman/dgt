/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_KV_APP_H_
#define PS_KV_APP_H_
#include <algorithm>
#include <utility>
#include <vector>
#include "ps/base.h"
#include "ps/simple_app.h"
#include <unistd.h>
#include "ps/internal/message.h"

#ifndef LITTLE_GRAIN_MSG
#define LITTLE_GRAIN_MSG
#endif
namespace ps {

/**
 * \brief the structure for a list of key-value pairs
 *
 * The keys must be unique and sorted in an increasing order.  The length of a
 * value can be more than one. If \a lens is empty, then the length
 * of a value is determined by `k=vals.size()/keys.size()`.  The \a i-th KV pair
 * is then
 *
 * \verbatim {keys[i], (vals[i*k], ..., vals[(i+1)*k-1])} \endverbatim
 *
 * If \a lens is given, then `lens[i]` is the length of the \a i-th
 * value. Let
 *
 * \verbatim n = lens[0] + .. + lens[i-1]  \endverbatim
 *
 * then the \a i-th KV pair is presented as
 *
 * \verbatim {keys[i], (vals[n], ..., vals[lens[i]+n-1])} \endverbatim
 */
template <typename Val>
struct KVPairs {
  // /** \brief empty constructor */
  // KVPairs() {}
  /** \brief the list of keys */
  SArray<Key> keys;
  /** \brief the according values */
  SArray<Val> vals;
  /** \brief the according value lengths (could be empty) */
  SArray<int> lens;
};

/**
 * \brief A worker node that can \ref Push (\ref Pull) key-value pairs to (from) server
 * nodes
 *
 * \tparam Val the type of value, which should be primitive types such as
 * int32_t and float
 */
template<typename Val>
class KVWorker : public SimpleApp {
 public:
  /** avoid too many this-> */
  using SimpleApp::obj_;
  /**
   * \brief callback function for \ref Push and \ref Pull
   *
   * It is called by the data receiving thread of this instance when the push or
   * pull is actually finished. Namely the kv pairs have already written into
   * servers' data structure or the kv pairs have already pulled back.
   */
  using Callback = std::function<void()>;

  /**
   * \brief constructor
   *
   * \param app_id the app id, should match with \ref KVServer's id
   * \param customer_id the customer id which is unique locally
   */
  explicit KVWorker(int app_id, int customer_id) : SimpleApp() {
    using namespace std::placeholders;
    slicer_ = std::bind(&KVWorker<Val>::DefaultSlicer, this, _1, _2, _3);
    obj_ = new Customer(app_id, customer_id, std::bind(&KVWorker<Val>::Process, this, _1));
  }

  /** \brief deconstructor */
  virtual ~KVWorker() { delete obj_; obj_ = nullptr; }

  /**
   * \brief Pushes a list of key-value pairs to all server nodes.
   *
   * This function pushes a KV list specified by \a keys and \a vals to all
   * server nodes.
   *
   * Sample usage: the following codes push two KV pairs `{1, (1.1, 1.2)}` and `{3,
   * (3.1,3.2)}` to server nodes, where the value is a length-2 float vector
   * \code
   *   KVWorker<float> w;
   *   std::vector<Key> keys = {1, 3};
   *   std::vector<float> vals = {1.1, 1.2, 3.1, 3.2};
   *   w.Push(keys, vals);
   * \endcode
   *
   * If \a lens is given, then the value can be various length. See
   * \ref KVPairs for more information.
   *
   * The KV list is partitioned and sent based on the key range each server
   * maintaining. This function returns without waiting the data are sent
   * actually. Instead, use either \ref Wait or the callback to know when
   * finished. This function is thread-safe.
   *
   * @param keys a list of keys, must be unique and sorted in increasing order
   * @param vals the according values
   * @param lens optional, lens[i] stores the value length of the \a
   * i-th KV pair
   * @param cmd an optional command sent to the servers
   * @param cb the callback which is called when the push is finished.
   * @return the timestamp of this request
   */
  int Push(const std::vector<Key>& keys,
           const std::vector<Val>& vals,
           const std::vector<int>& lens = {},
           int cmd = 0,
           const Callback& cb = nullptr) {
    return ZPush(
        SArray<Key>(keys), SArray<Val>(vals), SArray<int>(lens), cmd, cb);
  }

  /**
   * \brief Pulls the values associated with the keys from the server nodes
   *
   * This function pulls the values of the keys specified in \a keys from the
   * server nodes. The format is same to \ref KVPairs
   *
   * Sample usage: the following codes pull the values of keys \a 1 and \a 3
   * from the server nodes.
   * \code
   *   KVWorker<float> w;
   *   std::vector<Key> keys = {1, 3};
   *   std::vector<float> vals;
   *   ps.Pull(keys, &vals);
   * \endcode
   *
   * It's a non-blocking call. The actual pulling is finished,
   * namely \a vals (and \a lens) is filled with pulled values, only
   * if \ref Wait returns or the callback is called.
   *
   * @param keys a list of keys, must be unique and sorted in increasing order
   * @param vals the buffer for the pulled values. It can be 0 size.
   * @param lens optional buffer for the value length. If set, it can be 0 size.
   * @param cmd an optional command sent to the servers
   * @param cb the callback which is called when the pull is finished.
   * @return the timestamp of this request
   */
  int Pull(const std::vector<Key>& keys,
           std::vector<Val>* vals,
           std::vector<int>* lens = nullptr,
           int cmd = 0,
           const Callback& cb = nullptr) {
    return Pull_(SArray<Key>(keys), vals, lens, cmd, cb);
  }

  /**
   * \brief Waits until a push or pull has been finished
   *
   * Sample usage:
   * \code
   *   int ts = w.Pull(keys, &vals);
   *   Wait(ts);
   *   // now vals is ready for use
   * \endcode
   *
   * \param timestamp the timestamp returned by the push or pull
   */
  void Wait(int timestamp) { obj_->WaitRequest(timestamp); }

  /**
   * \brief zero-copy Push
   *
   * This function is similar to \ref Push except that all data
   * will not be copied into system for better performance. It is the caller's
   * responsibility to keep the content to be not changed before actually
   * finished.
   */
  int ZPush(const SArray<Key>& keys,
            const SArray<Val>& vals,
            const SArray<int>& lens = {},
            int cmd = 0,
            const Callback& cb = nullptr) {
#ifdef LITTLE_GRAIN_MSG
	int ts = obj_->NewRequest(kServerGroup, keys.size());
#else
    int ts = obj_->NewRequest(kServerGroup);
#endif
    AddCallback(ts, cb);
    KVPairs<Val> kvs;
    kvs.keys = keys;
    kvs.vals = vals;
    kvs.lens = lens;
    Send(ts, true, cmd, kvs);
    return ts;
  }

  /**
   * \brief zero-copy Pull
   *
   * This function is similar to \ref Pull except that all data
   * will not be copied into system for better performance. It is the caller's
   * responsibility to keep the content to be not changed before actually
   * finished.
   */
  int ZPull(const SArray<Key>& keys,
            SArray<Val>* vals,
            SArray<int>* lens = nullptr,
            int cmd = 0,
            const Callback& cb = nullptr) {
    return Pull_(keys, vals, lens, cmd, cb);
  }
  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;
  /**
   * \brief a slicer partitions a key-value list according to the key ranges
   * \param send the kv list for partitioning
   * \param ranges the key ranges, ranges[i] is the key range of server i
   * \param sliced the sliced lists. slices[i] should only contains keys in
   * ranges[i] and the according values
   */
  using Slicer = std::function<void(
      const KVPairs<Val>& send, const std::vector<Range>& ranges,
      SlicedKVs* sliced)>;

  /**
   * \brief set a user-defined slicer
   */
  void set_slicer(const Slicer& slicer) {
    CHECK(slicer); slicer_ = slicer;
  }

 private:
  /**
   * \brief internal pull, C/D can be either SArray or std::vector
   */
  template <typename C, typename D>
  int Pull_(const SArray<Key>& keys, C* vals, D* lens,
            int cmd, const Callback& cb);
  /**
   * \brief add a callback for a request. threadsafe.
   * @param cb callback
   * @param timestamp the timestamp of the request
   */
  void AddCallback(int timestamp, const Callback& cb) {
    if (!cb) return;
    std::lock_guard<std::mutex> lk(mu_);
    callbacks_[timestamp] = cb;
  }

  /**
   * \brief run and delete the callback
   * \param timestamp the timestamp of the callback
   */
  void RunCallback(int timestamp);
  /**
   * \brief send the kv list to all servers
   * @param timestamp the timestamp of the request
   * @param push whether or not it is a push request
   * @param cmd command
   */
  void Send(int timestamp, bool push, int cmd, const KVPairs<Val>& kvs);
  /** \brief internal receive handle */
  void Process(const Message& msg);
  /** \brief default kv slicer */
  void DefaultSlicer(const KVPairs<Val>& send,
                     const std::vector<Range>& ranges,
                     SlicedKVs* sliced);

  /** \brief data buffer for received kvs for each timestamp */
  std::unordered_map<int, std::vector<KVPairs<Val>>> recv_kvs_;
  /** \brief callbacks for each timestamp */
  std::unordered_map<int, Callback> callbacks_;
  /** \brief lock */
  std::mutex mu_;
  /** \brief kv list slicer */
  Slicer slicer_;
};

/** \brief meta information about a kv request */
struct KVMeta {
  /** \brief the int cmd */
  int cmd;
  /** \brief whether or not this is a push request */
  bool push;
  /** \brief sender's node id */
  int sender;
  /** \brief the associated timestamp */
  int timestamp;
#ifdef LITTLE_GRAIN_MSG
  int tracker_num;
#endif
#ifdef UDP_CHANNEL
  int first_key;
#endif
  /** \brief the customer id of worker */
  int customer_id;
};

/**
 * \brief A server node for maintaining key-value pairs
 */
template <typename Val>
class KVServer : public SimpleApp {
 public:
  /**
   * \brief constructor
   * \param app_id the app id, should match with \ref KVWorker's id
   */
  explicit KVServer(int app_id) : SimpleApp() {
    using namespace std::placeholders;
    obj_ = new Customer(app_id, app_id, std::bind(&KVServer<Val>::Process, this, _1));
  }

  /** \brief deconstructor */
  virtual ~KVServer() { delete obj_; obj_ = nullptr; }

  /**
   * \brief the handle to process a push/pull request from a worker
   * \param req_meta meta-info of this request
   * \param req_data kv pairs of this request
   * \param server this pointer
   */
  using ReqHandle = std::function<void(const KVMeta& req_meta,
                                       const KVPairs<Val>& req_data,
                                       KVServer* server)>;
  void set_request_handle(const ReqHandle& request_handle) {
    CHECK(request_handle) << "invalid request handle";
    request_handle_ = request_handle;
  }

  /**
   * \brief response to the push/pull request
   * \param req the meta-info of the request
   * \param res the kv pairs that will send back to the worker
   */
  void Response(const KVMeta& req, const KVPairs<Val>& res = KVPairs<Val>());

 private:
  /** \brief internal receive handle */
  void Process(const Message& msg);
  /** \brief request handle */
  ReqHandle request_handle_;
};


/**
 * \brief an example handle adding pushed kv into store
 */
template <typename Val>
struct KVServerDefaultHandle {
  void operator()(
      const KVMeta& req_meta, const KVPairs<Val>& req_data, KVServer<Val>* server) {
    size_t n = req_data.keys.size();
    KVPairs<Val> res;
    if (req_meta.push) {
      CHECK_EQ(n, req_data.vals.size());
    } else {
      res.keys = req_data.keys; res.vals.resize(n);
    }
    for (size_t i = 0; i < n; ++i) {
      Key key = req_data.keys[i];
      if (req_meta.push) {
        store[key] += req_data.vals[i];
      } else {
        res.vals[i] = store[key];
      }
    }
    server->Response(req_meta, res);
  }
  std::unordered_map<Key, Val> store;
};


///////////////////////////////////////////////////////////////////////////////

template <typename Val>
void KVServer<Val>::Process(const Message& msg) {
  if (msg.meta.simple_app) {
    SimpleApp::Process(msg); return;
  }
  KVMeta meta;
  meta.cmd       = msg.meta.head;
  meta.push      = msg.meta.push;
  meta.sender    = msg.meta.sender;
  meta.timestamp = msg.meta.timestamp;
#ifdef LITTLE_GRAIN_MSG
  meta.tracker_num = msg.meta.tracker_num;
#endif
#ifdef UDP_CHANNEL
  meta.first_key = msg.meta.first_key;
#endif
  meta.customer_id = msg.meta.customer_id;
  //printf("#Process 387:timestamp = %d,meta.tracker_num = %d\n",meta.timestamp,meta.tracker_num);
  KVPairs<Val> data;
  int n = msg.data.size();
  if (n) {
    CHECK_GE(n, 2);
    data.keys = msg.data[0];
    data.vals = msg.data[1];
    if (n > 2) {
      CHECK_EQ(n, 3);
      data.lens = msg.data[2];
      CHECK_EQ(data.lens.size(), data.keys.size());
    }
  }
  CHECK(request_handle_);
  static int count = 0;
  count++;
  //std::cout << "Enter KVServer::Process: " << count << std::endl;
  request_handle_(meta, data, this);
}

template <typename Val>
void KVServer<Val>::Response(const KVMeta& req, const KVPairs<Val>& res) {
  Message msg;
  msg.meta.app_id = obj_->app_id();
  msg.meta.customer_id = req.customer_id;
  msg.meta.request     = false;
  msg.meta.push        = req.push;
  msg.meta.head        = req.cmd;
  msg.meta.timestamp   = req.timestamp;
#ifdef LITTLE_GRAIN_MSG
  msg.meta.tracker_num = req.tracker_num;
#endif
#ifdef UDP_CHANNEL
  msg.meta.first_key = req.first_key;
#endif
  msg.meta.recver      = req.sender;
  if (res.keys.size()) {
    msg.AddData(res.keys);
#ifdef UDP_CHANNEL
	msg.meta.keys_len = msg.data.back().size();
#endif
	//std::cout << "kv_app.h#402: " << res.keys << std::endl;
    msg.AddData(res.vals);
#ifdef UDP_CHANNEL
	msg.meta.vals_len = msg.data.back().size();
#endif
    if (res.lens.size()) {
	  //std::cout << "kv_app.h#405: " << res.lens << std::endl;
      msg.AddData(res.lens);
#ifdef UDP_CHANNEL
	  msg.meta.lens_len = msg.data.back().size();
#endif
    }
  }
  /* int n = msg.data.size();
  msg.meta.data_num = n;
  msg.meta.data_len.clear();
	for(int i = 0; i < n; ++i){
		msg.meta.data_len.push_back(msg.data[i].size());
	} */
  Postoffice::Get()->van()->Send(msg);
}

template <typename Val>
void KVWorker<Val>::DefaultSlicer(
    const KVPairs<Val>& send, const std::vector<Range>& ranges,
    typename KVWorker<Val>::SlicedKVs* sliced) {
//#ifdef LITTLE_GRAIN_MSG
	/* sliced->resize(send.keys.size());
	size_t n = send.keys.size();
	size_t val_begin = 0, val_end = 0;
	size_t k = send.vals.size() / send.keys.size();
	for (size_t i = 0; i < n; ++i) {
		sliced->at(i).first = true;
		auto& kv = sliced->at(i).second;
		kv.keys = send.keys.segment(i,i+1);
		if (send.lens.size()) {
			kv.lens = send.lens.segment(i,i+1);
			val_end += send.lens[i];
			kv.vals = send.vals.segment(val_begin, val_end);
			val_begin = val_end;
		}else{
			kv.vals = send.vals.segment(i*k, (i+1)*k);
		}
	} */
//#else
  sliced->resize(ranges.size());

  // find the positions in msg.key
  size_t n = ranges.size();
  std::vector<size_t> pos(n+1);
  const Key* begin = send.keys.begin();
  const Key* end = send.keys.end();
  for (size_t i = 0; i < n; ++i) {
    if (i == 0) {
      pos[0] = std::lower_bound(begin, end, ranges[0].begin()) - begin;
      begin += pos[0];
    } else {
      CHECK_EQ(ranges[i-1].end(), ranges[i].begin());
    }
    size_t len = std::lower_bound(begin, end, ranges[i].end()) - begin;
    begin += len;
    pos[i+1] = pos[i] + len;

    // don't send it to servers for empty kv
    sliced->at(i).first = (len != 0);
  }
  CHECK_EQ(pos[n], send.keys.size());
  if (send.keys.empty()) return;

  // the length of value
  size_t k = 0, val_begin = 0, val_end = 0;
  if (send.lens.empty()) {
    k = send.vals.size() / send.keys.size();
    CHECK_EQ(k * send.keys.size(), send.vals.size());
  } else {
    CHECK_EQ(send.keys.size(), send.lens.size());
  }

  // slice
  for (size_t i = 0; i < n; ++i) {
    if (pos[i+1] == pos[i]) {
      sliced->at(i).first = false;
      continue;
    }
    sliced->at(i).first = true;
    auto& kv = sliced->at(i).second;
    kv.keys = send.keys.segment(pos[i], pos[i+1]);
    if (send.lens.size()) {
      kv.lens = send.lens.segment(pos[i], pos[i+1]);
      for (int l : kv.lens) val_end += l;
      kv.vals = send.vals.segment(val_begin, val_end);
      val_begin = val_end;
    } else {
      kv.vals = send.vals.segment(pos[i]*k, pos[i+1]*k);
    }
  }
//#endif
}

template <typename Val>
void KVWorker<Val>::Send(int timestamp, bool push, int cmd, const KVPairs<Val>& kvs) {
  // slice the message
  SlicedKVs sliced;
  slicer_(kvs, Postoffice::Get()->GetServerKeyRanges(), &sliced);


#ifndef LITTLE_GRAIN_MSG
  // need to add response first, since it will not always trigger the callback
  int skipped = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (!sliced[i].first) ++skipped;
  }
  obj_->AddResponse(timestamp, skipped);
  if ((size_t)skipped == sliced.size()) {
    RunCallback(timestamp);
  }
#endif
  if(push == true){
	//std::cout << "slice.size = " << sliced.size() << std::endl;
	//std::cout << "kv_app.h #529" << std::endl;
  }
  //std::cout << "kv_app.h #530\n" << std::endl;
  for (size_t i = 0; i < sliced.size(); ++i) {
	
    const auto& s = sliced[i];
    if (!s.first) continue;
#ifdef LITTLE_GRAIN_MSG
	
	const auto& tmp_kvs = s.second;
	size_t n = tmp_kvs.keys.size();
	size_t val_begin = 0, val_end = 0;
	size_t k = tmp_kvs.vals.size() / tmp_kvs.keys.size();
	for (size_t j = 0; j < n; ++j) {
		Message msg;
		msg.meta.app_id = obj_->app_id();
		msg.meta.customer_id = obj_->customer_id();
		msg.meta.request     = true;
		msg.meta.push        = push;
		msg.meta.head        = cmd;
		//msg.meta.control.cmd = Control::DATA;
		msg.meta.timestamp   = timestamp;
		msg.meta.tracker_num = kvs.keys.size();
		msg.meta.recver      = Postoffice::Get()->ServerRankToID(i);
		//msg.meta.sender      = Postoffice::Get()->van()->my_node().id;
		KVPairs<char> tmp_kv;
		tmp_kv.keys = tmp_kvs.keys.segment(j,j+1);
		msg.meta.first_key = tmp_kv.keys[0];
		if (tmp_kvs.lens.size()) {
			tmp_kv.lens = tmp_kvs.lens.segment(j,j+1);
			val_end += tmp_kvs.lens[j];
			tmp_kv.vals = tmp_kvs.vals.segment(val_begin, val_end);
			val_begin = val_end;
		}else{
			tmp_kv.vals = tmp_kvs.vals.segment(j*k, (j+1)*k);
		}
/* #ifdef UDP_CHANNEL
		msg.meta.data_num = 0;
#endif */
		if (tmp_kv.keys.size()) {

		  msg.AddData(tmp_kv.keys);
#ifdef UDP_CHANNEL
		  msg.meta.keys_len = msg.data.back().size();
#endif

		  if(msg.meta.push == true){
			/*   std::cout << "recver" << msg.meta.recver << std::endl;
			  std::cout << tmp_kv.keys << std::endl; */
		  }
		  msg.AddData(tmp_kv.vals);
#ifdef UDP_CHANNEL
		 msg.meta.vals_len = msg.data.back().size();
#endif
		  if (tmp_kv.lens.size()) {
			msg.AddData(tmp_kv.lens);
#ifdef UDP_CHANNEL
			msg.meta.lens_len = msg.data.back().size();
#endif
			if(msg.meta.push == true){
				/* std::cout << tmp_kv.lens << std::endl; */
			}
		  }
		}
		/* if(msg.meta.push == true){
			std::cout << "-----------------------------------------------" << std::endl;
		} */
		//printf("################################\n");
		//std::cout << "msg.meta.timestamp = " <<msg.meta.timestamp << " tracker_num = " << msg.meta.tracker_num << std::endl;
		//usleep(100000);
/* #ifdef UDP_CHANNEL
		int n = msg.data.size();
		msg.meta.data_num = n;
		for(int i = 0; i < n; ++i){
			msg.meta.data_len.push_back(msg.data[i].size());
		}
		std::cout << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" << std::endl;
		  std::cout << "msg->meta.data_num = " << msg.meta.data_num << std::endl;
		    for(int i = 0; i < msg.meta.data_num; ++i){
				std::cout << "msg->meta.data_len[i]"  << msg.meta.data_len[i] << std::endl;
			}
#endif */
		
		usleep(100);
		Postoffice::Get()->van()->Send(msg);
	}
#else
    Message msg;
    msg.meta.app_id = obj_->app_id();
    msg.meta.customer_id = obj_->customer_id();
    msg.meta.request     = true;
    msg.meta.push        = push;
    msg.meta.head        = cmd;
    msg.meta.timestamp   = timestamp;
    msg.meta.recver      = Postoffice::Get()->ServerRankToID(i);
    const auto& kvs = s.second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
	  if(msg.meta.push == true){
		  std::cout << "recver" << msg.meta.recver << std::endl;
		  std::cout << kvs.keys << std::endl;
	  }
      msg.AddData(kvs.vals);
      if (kvs.lens.size()) {
        msg.AddData(kvs.lens);
		if(msg.meta.push == true){
			std::cout << kvs.lens << std::endl;
		}
      }
    }
	if(msg.meta.push == true){
	std::cout << "-----------------------------------------------" << std::endl;
	}
    Postoffice::Get()->van()->Send(msg);
#endif
  }
  
}



template <typename Val>
void KVWorker<Val>::Process(const Message& msg) {
  if (msg.meta.simple_app) {
    SimpleApp::Process(msg); return;
  }
  // store the data for pulling
  int ts = msg.meta.timestamp;
  if (!msg.meta.push && msg.data.size()) {
    CHECK_GE(msg.data.size(), (size_t)2);
    KVPairs<Val> kvs;
	if(msg.data.size() > 0){
		  if(((SArray<Key>)msg.data[0])[0] > 100000){
				std::cout << "KVWorker->Process#678:find error key" << (SArray<Key>)msg.data[0] << msg.meta.DebugString() << std::endl;
		   }
		   KVPairs<Val> test_kvs;
		   test_kvs.keys = msg.data[0];
		   if(test_kvs.keys[0] > 100000){
		   
		   std::cout << "KVWorker->Process#683:test_kvs:" << test_kvs.keys << msg.meta.DebugString() << std::endl;
		   std::cout << "KVWorker->Process#683:msg.data[0][0] = " << (SArray<Key>)msg.data[0] << std::endl;
	}
		  
	  }
    kvs.keys = msg.data[0];
	if(kvs.keys[0] > 100000){
		std::cout << "find error key" << kvs.keys << msg.meta.DebugString() << std::endl;
	}
    kvs.vals = msg.data[1];
    if (msg.data.size() > (size_t)2) {
      kvs.lens = msg.data[2];
    }
    mu_.lock();
    recv_kvs_[ts].push_back(kvs);
    mu_.unlock();
  }

  // finished, run callbacks
#ifdef LITTLE_GRAIN_MSG
	//std::cout << obj_->NumResponse(ts) << "---" << msg.meta.tracker_num << std::endl;
	if (obj_->NumResponse(ts) == msg.meta.tracker_num-1)  {
		//std::cout << "#635:Prepare to call RunCallback" << std::endl;
		RunCallback(ts);
	}
#else
  if (obj_->NumResponse(ts) == Postoffice::Get()->num_servers() - 1)  {
    RunCallback(ts);
  }
#endif
}
template <typename Val>
void KVWorker<Val>::RunCallback(int timestamp) {
  mu_.lock();
  auto it = callbacks_.find(timestamp);
  if (it != callbacks_.end()) {
    mu_.unlock();

    CHECK(it->second);
    it->second();

    mu_.lock();
    callbacks_.erase(it);
  }
  mu_.unlock();
}

template <typename Val>
template <typename C, typename D>
int KVWorker<Val>::Pull_(
    const SArray<Key>& keys, C* vals, D* lens, int cmd, const Callback& cb) {
#ifdef LITTLE_GRAIN_MSG
  int ts = obj_->NewRequest(kServerGroup, keys.size());
#else
  int ts = obj_->NewRequest(kServerGroup);
#endif
  AddCallback(ts, [this, ts, keys, vals, lens, cb]() mutable {
      mu_.lock();
      auto& kvs = recv_kvs_[ts];
      mu_.unlock();

      // do check
      size_t total_key = 0, total_val = 0;
      for (const auto& s : kvs) {
        Range range = FindRange(keys, s.keys.front(), s.keys.back()+1);
        CHECK_EQ(range.size(), s.keys.size())
            << "unmatched keys size from one server" << keys <<"("<<range.begin() << "," << range.end() << ")"<<s.keys;

        if (lens) CHECK_EQ(s.lens.size(), s.keys.size());

		//std::cout << "keys.size:"<<s.keys.size()<<"vals.size:"<<s.vals.size()<< "len.size:"<<s.lens.size()<<std::endl;
        total_key += s.keys.size();
        total_val += s.vals.size();
      }
	  
      CHECK_EQ(total_key, keys.size()) << "lost some servers?";

      // fill vals and lens
      std::sort(kvs.begin(), kvs.end(), [](
          const KVPairs<Val>& a, const KVPairs<Val>& b) {
                  return a.keys.front() < b.keys.front();
        });
      CHECK_NOTNULL(vals);
      if (vals->empty()) {
        vals->resize(total_val);
      } else {
        CHECK_EQ(vals->size(), total_val);
      }
      Val* p_vals = vals->data();
      int *p_lens = nullptr;
      if (lens) {
        if (lens->empty()) {
          lens->resize(keys.size());
        } else {
          CHECK_EQ(lens->size(), keys.size());
        }
        p_lens = lens->data();
      }
      for (const auto& s : kvs) {
        memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
        p_vals += s.vals.size();
        if (p_lens) {
          memcpy(p_lens, s.lens.data(), s.lens.size() * sizeof(int));
          p_lens += s.lens.size();
        }
      }

      mu_.lock();
      recv_kvs_.erase(ts);
      mu_.unlock();
      if (cb) cb();
    });

  KVPairs<Val> kvs; kvs.keys = keys;
  Send(ts, false, cmd, kvs);
  return ts;
}

}  // namespace ps
#endif  // PS_KV_APP_H_
