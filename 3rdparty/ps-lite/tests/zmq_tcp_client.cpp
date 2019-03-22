#include <zmq.h>
#include <stdlib.h>
#include <thread>
#include <string>

#include <unistd.h>
#include <string.h>
#include <assert.h>
//#define ZMQ_RADIO 14
//#define ZMQ_DISH 15
class ZMQVan{
public:
	ZMQVan(){}
	virtual ~ZMQVan(){}
	
	void Start(){
		if(context_ == nullptr){
			context_ = zmq_ctx_new();
			if(context_ == NULL){
				printf("creat zmq context failed\n");
				return;
			}
			zmq_ctx_set(context_, ZMQ_MAX_SOCKETS,65535);	
		}
	}

	void Bind(){
		printf("Enter Bind\n");
		int max_retry = 5;
		receiver_ = zmq_socket(context_, ZMQ_REP);
		if(receiver_ == NULL){
			printf("create receiver socket failed\n");
			return;
		}
		std::string addr = "tcp://*:";
		unsigned seed = static_cast<unsigned>(time(NULL));
		srand(seed);
		int port = 6666;
	    for(int i=0;i<max_retry; i++){
		    auto address = addr+std::to_string(port);	
			if(zmq_bind(receiver_, address.c_str()) == 0){
				printf("Bind success, Listen on %d\n",port);
				break;
			}
			port = 10000+rand()%40000;
			if(i == max_retry-1){
				printf("Bind failed\n");
				break;
			}

		}
	}
	void Connect(char* port){
		sender_ = zmq_socket(context_, ZMQ_RADIO);
		if(sender_ == NULL){
			printf("create sender socket failed\n");
			return;
		}
		std::string my_id = "ps9";
		zmq_setsockopt(sender_, ZMQ_IDENTITY, my_id.data(), my_id.size());
		std::string addr = "udp://localhost:"+std::string(port);
		if(zmq_connect(sender_, addr.c_str()) !=0){
			printf("connect to %s failed\n", addr.c_str());
			return;
		}else{
			printf("Connect to %s success\n",addr.c_str());
		}
	}

	inline void my_free (void *data, void *hint){

		free (data);
	}
	
	int SendMsg(int mode){
		int rc;
		int tag = 0;
		printf("SendMsg work on mode %d\n",mode);
		if(mode == 0){
			std::string send_buf = "Hello ABCDEF";
			zmq_msg_t data_msg;
			zmq_msg_t recv_msg;
			zmq_msg_init(&recv_msg);
			for(int request_nbr = 0; request_nbr<20; request_nbr++){
			   //zmq_msg_copy(&meta_msg_t, &meta_msg);
			   //printf("Copy complete\n");
			   zmq_msg_init_data(&data_msg, (char*)send_buf.data(), send_buf.size(), NULL, NULL);
			   zmq_msg_set_group (&data_msg, "TV");
			   rc = zmq_msg_send(&data_msg, sender_, 0);
			  // assert(rc == send_buf.size());
			   if(rc != -1){
				printf("#MODE0:Send Message:%s, size:%d Bytes\n",(char*)send_buf.data(), rc);
				}else{
					printf("#MODE0:Send Message:%s, FAILED\n",(char*)send_buf.data());
				}
				sleep(1);
			   /*  rc = zmq_msg_recv(&recv_msg, sender_, 0);
				printf("GET:%s\n",(char*)zmq_msg_data(&recv_msg));  */
			}
			
			
			

		}else if(mode == 1){
			char buffer[10];
			void *data = malloc(10);
			memcpy(data, "ABCDEF",6);
			for(int i = 0; i<10; i++){
				 rc = zmq_send(sender_, data, 6, 0);
				 if(rc != -1){

					 printf("#MODE1:Send Message:%s, size:%d Bytes\n",(char*)data,rc);
				 }else{
					 printf("#MODE1:Send Message:%s, FAILED\n",(char*)data);
				 }
				 if(zmq_recv(sender_, buffer, 10, 0) != -1){
					 printf("#MODE1:Recvive:%s\n",buffer);
				 }
			}
			
		}
		return rc;
			
	}

	int RecvMsg(int mode){
		int rc;
		if(mode == 0){
			zmq_msg_t* zmsg = new zmq_msg_t;
			zmq_msg_init(zmsg);
			while(1){
				rc = zmq_msg_recv(zmsg, receiver_, 0);
				printf("Recved\n");
				printf("#MODE0:Recv Message:%s, size:%d Bytes\n",(char *)zmq_msg_data(zmsg),zmq_msg_size(zmsg));

			}
			delete zmsg;
		}else if(mode == 1){
			char buffer[10];		
			rc = zmq_recv(receiver_, buffer, 10, 0);
			printf("#MODE1:Recv Message:%s, size:%d Bytes\n", buffer, rc);
			sleep(1);
			rc = zmq_send(receiver_, "ACK", 3,0);
		}
		return rc;
		
	}
    	
	void Stop(){
		//close socket
		int linger = 0;
		if(sender_ != nullptr){
			zmq_setsockopt(sender_, ZMQ_LINGER, &linger, sizeof(linger));
			zmq_close(sender_);
		}
		if(receiver_ != nullptr){
			zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
			zmq_close(receiver_);
		}
			zmq_ctx_destroy(context_);
	}
  	
private:
	void* sender_=nullptr;
	void* receiver_ = nullptr;
	
	void* context_;

};




int main(int argc,char* argv[])
{
	ZMQVan* van = new ZMQVan();	
	van->Start();
	van->Connect(argv[2]);
	van->SendMsg(std::atoi(argv[1]));
	van->Stop();
	return 0;
}
