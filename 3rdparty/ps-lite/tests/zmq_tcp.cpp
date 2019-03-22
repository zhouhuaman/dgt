#include "../include/zmq.h"
#include <stdlib.h>
#include <thread>
#include <string>

#include <unistd.h>
#include <string.h>
#include <assert.h>
//#include "testutil.hpp"
//#include "testutil_unity.hpp"
//#include <unity.h>
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
		int rc;
		printf("Enter Bind\n");
		int max_retry = 5;
		receiver_ = zmq_socket(context_, ZMQ_DISH);
		if(receiver_ == NULL){
			printf("create receiver socket failed\n");
			return;
		}
		std::string addr = "udp://*:";
		unsigned seed = static_cast<unsigned>(time(NULL));
		srand(seed);
		int port = 7777;
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
		
	 	rc = zmq_join(receiver_, "TV");	
		assert(rc == 0);
	}
	void Connect(){
		sender_ = zmq_socket(context_, ZMQ_REQ);
		if(sender_ == NULL){
			printf("create sender socket failed\n");
			return;
		}
		std::string my_id = "ps9";
		zmq_setsockopt(sender_, ZMQ_IDENTITY, my_id.data(), my_id.size());
		std::string addr = "tcp://localhost:6666";
		if(zmq_connect(sender_, addr.c_str()) !=0){
			printf("connect to %s failed\n", addr.c_str());
			return;
		}
	}

	int SendMsg(int mode){
		int rc;
		int tag = 0;
		zmq_msg_t data_msg;
		if(mode == 0){
			void *data = malloc (6);
			memcpy (data, "ABCDEF", 6);
			zmq_msg_init_data(&data_msg, data, 6, NULL, NULL);
			rc = zmq_msg_send(&data_msg, sender_, tag);
			if(rc != -1){
				printf("#MODE0:Send Message:%s, size:%d Bytes\n",(char*)data, rc);
			}else{
				printf("#MODE0:Send Message:%s, FAILED\n",(char*)data);
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
		printf("RecvMsg work on mode %d\n",mode);
		if(mode == 0){
			zmq_msg_t zmsg;
			zmq_msg_t ack_msg;
			rc = zmq_msg_init(&zmsg);
			assert(rc == 0);
			std::string meta_buf = "ACK";
			
			while(1){
				rc = zmq_msg_recv(&zmsg, receiver_, 0);
				assert(rc != -1);
				printf("Recved\n");
				printf("#MODE0:Recv Message:%s,%s, size:%d Bytes\n",(char*)zmq_msg_group (&zmsg),(char *)zmq_msg_data(&zmsg),zmq_msg_size(&zmsg));
			     	
				//sleep(1);//do some work
				/* zmq_msg_init_data(&ack_msg, (char *)meta_buf.data(), meta_buf.size(), NULL,NULL);
				rc = zmq_msg_send(&ack_msg, receiver_, 0);  */
			}
		}else if(mode == 1){
			char buffer[10];		
			while(1){
				rc = zmq_recv(receiver_, buffer, 10, 0);
				printf("#MODE1:Recv Message:%s, size:%d Bytes\n", buffer, rc);
				sleep(1);
				rc = zmq_send(receiver_, "ACK", 3,0);
			}
		}else if(mode == 2){
			/*zmq_msg_t zmsg;
			rc = zmq_msg_init(&zmsg);
			assert(rc == 0);
			rc = zmq_recv(receiver_, &zmsg, 0);
			assert(rc==0);
			printf("#MODE2:Recv Message:%s, size:%d Bytes\n",(char*)zmq_msg_data(&zmsg),zmq_msg_size(&zmsg));
			zmq_msg_close(&zmsg);
			*/
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




int main(int argc, char* argv[])
{
	ZMQVan* van = new ZMQVan();	
	van->Start();
	van->Bind();
	van->RecvMsg(std::atoi(argv[1]));
	van->Stop();
	return 0;
}
