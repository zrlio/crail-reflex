#include "stanford_mast_reflex_NativeDispatcher.h"
#include <unistd.h>


#include <sched.h>
#include <assert.h>

#include <iostream>
#include <sstream>

#include <cstdio>
#include <cstring>
#include <cerrno>
#include <cstdlib>
//#include <spdk/nvme.h>
//#include <spdk/env.h>
//#include <spdk/nvme_intel.h>
//#include <spdk/pci_ids.h>


// spdk is missing extern C in some headers
//extern "C" {
//#include <spdk/log.h>
//#include <nvme_internal.h>
//}

#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>

#include <arpa/inet.h>

#include <netinet/tcp.h>

#include <sys/prctl.h>

//#include <rte_config.h>
//#include <rte_lcore.h>


#define PACKAGE_NAME "stanford/mast/reflex"

#define SECTOR_SIZE 512 

#define NUM_WORKERS 1
//per worker
struct worker {
	int cpu;
	struct event_base *base;
	struct bufferevent *bev;
	pthread_t tid;
} workers[NUM_WORKERS];

struct completed_array {
    int index;
    long ids[0];
};

struct io_completion {
    int status_code_type;
    int status_code;
    const long id;
    completed_array* completed;
};

struct completion { //need to store req_handle and io_compl_addr
	void* req_handle;
	void* io_compl_addr;
};

typedef struct __attribute__ ((__packed__)) {
  uint16_t magic;
  uint16_t opcode;
  struct completion* compl_ctx; //void* req_handle in ReFlex header
  unsigned long lba;
  unsigned int lba_count;
} binary_header_blk_t;

struct nvme_req {
    uint8_t cmd;
    unsigned long lba;
    unsigned int lba_count;
	long handle;
    unsigned long sent_time;
    // nvme buffer to read/write data into
    char buf[4096]; //FIXME: update for sgl
};

/*
 * Memcached protocol support 
 */

#define CMD_GET  0x00
#define CMD_SET  0x01
#define CMD_SET_NO_ACK  0x02
 
#define RESP_OK 0x00
#define RESP_EINVAL 0x04

#define REQ_PKT 0x80
#define RESP_PKT 0x81
#define MAX_EXTRA_LEN 8
#define MAX_KEY_LEN 8


int num_completions = 0;


class JNIString {
    private:
        jstring str_;
        JNIEnv* env_;
        const char* c_str_;
    public:
        JNIString(JNIEnv* env, jstring str) : str_(str), env_(env),
        c_str_(env_->GetStringUTFChars(str, NULL)) {}

        ~JNIString() {
            if (c_str_ != NULL) {
                env_->ReleaseStringUTFChars(str_, c_str_);
            }
        }

        const char* c_str() const {
            return c_str_;
        }
};



/*
 * Class:     stanford_mast_reflex_NativeDispatcher
 * Method:    _malloc
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_stanford_mast_reflex_NativeDispatcher__1malloc
  (JNIEnv *env, jobject obj, jlong size, jlong alignment)
{
	void * ret = malloc((size_t) size);
	if (!ret){
		printf("ERROR: malloc could not allocate memory\n");
	}
	return (jlong) ret;
}

/*
 * Class:     stanford_mast_reflex_NativeDispatcher
 * Method:    _free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_stanford_mast_reflex_NativeDispatcher__1free
  (JNIEnv *env, jobject obj, jlong addr)
{
	free((void*) addr);
	return;
}

/*
 * Class:     stanford_mast_reflex_NativeDispatcher
 * Method:    _hello_reflex
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_stanford_mast_reflex_NativeDispatcher__1hello_1reflex
  (JNIEnv *env, jobject obj)
{
	printf("Hello ReFlex!\n");
}


void connect_cb(struct bufferevent *bev, short events, void *ptr)
{
	if (events & BEV_EVENT_CONNECTED) {
		printf("connected to the server!\n");
    } else if (events & BEV_EVENT_ERROR) {
    	printf("could not connect to ReFlex server\n");
	}
}

// read data from socket
void read_cb(struct bufferevent *bev, void *ptr) {

	struct evbuffer *input = bufferevent_get_input(bev);

	int length = evbuffer_get_length(input);
  	if (length < sizeof(binary_header_blk_t)) return;

	binary_header_blk_t *header = new binary_header_blk_t;
	evbuffer_copyout(input, header, sizeof(binary_header_blk_t));

	if (header->opcode == CMD_SET){
		evbuffer_drain(input, sizeof(binary_header_blk_t));
		num_completions++;
		//printf("received SET resp\n");
		
		volatile io_completion* completion;
		completion = reinterpret_cast<volatile io_completion*>(header->compl_ctx->io_compl_addr);

		completion->status_code = 0; //FIXME: nvme_completion->status.sc;
		completion->status_code_type = 0; //FIXME: nvme_completion->status.sct;
		completed_array* ca = completion->completed;
		ca->ids[ca->index++] = completion->id;

		if (length > sizeof(binary_header_blk_t)){ //received more than one request, so process the rest
			read_cb(bev, NULL);
		}
		
		return;
	}
	
	// process CMD_GET response
	int datalen = header->lba_count * SECTOR_SIZE;
	
	int total_length = sizeof(binary_header_blk_t) + datalen; 
	if (length < total_length) return;

	//assert(length == total_length);

	void* addr_ctx = header->compl_ctx->req_handle;
	evbuffer_drain(input, sizeof(binary_header_blk_t));
	evbuffer_remove(input, addr_ctx, datalen);
	num_completions++;
 
	volatile io_completion* completion;
	completion = reinterpret_cast<volatile io_completion*>(header->compl_ctx->io_compl_addr);
    
	completion->status_code = 0; //FIXME: nvme_completion->status.sc;
    completion->status_code_type = 0; //FIXME: nvme_completion->status.sct;
    completed_array* ca = completion->completed;
    ca->ids[ca->index++] = completion->id;

	if (length > total_length){ //received more than one request, so process the rest
		read_cb(bev, NULL);
	}
	delete header->compl_ctx;
	//printf("received GET resp\n");

}

// written data to socket
void write_cb(struct bufferevent *bev, void *ptr) {
	//printf("write_cb\n");
}


static void set_affinity(int cpu)
{
	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);
	CPU_SET(cpu, &cpu_set);
	if (sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set) != 0) {
		perror("sched_setaffinity");
		exit(1);
	}
}


/*
 * Class:     stanford_mast_reflex_NativeDispatcher
 * Method:    _connect
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_stanford_mast_reflex_NativeDispatcher__1connect
  (JNIEnv *env, jobject obj, jlong ip_addr, jint port)
{
	printf("connect(): begin libevent init....\n");

	struct worker *worker;
	struct sockaddr_in sin;
	struct bufferevent *bev;
	int ret, i;

	worker = &workers[0]; //FIXME should be cpu id instead of 0 (only works with one thread for now)
	worker->cpu = sched_getcpu(); 	 //FIXME: should get CPU
	printf("worker on cpu %d\n", worker->cpu);

	set_affinity(worker->cpu);

	worker->base = event_base_new();
	if (!worker->base) {
		puts("Couldn't open event base");
		exit(1);
	}
	
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(ip_addr); 
	sin.sin_port = htons(port);


    bev = bufferevent_socket_new(worker->base, -1, BEV_OPT_CLOSE_ON_FREE);
	worker->bev = bev;

    bufferevent_setcb(bev, read_cb, write_cb, connect_cb, NULL);
    bufferevent_enable(bev, EV_READ | EV_WRITE);

    if (bufferevent_socket_connect(bev,
        (struct sockaddr *)&sin, sizeof(sin)) < 0) {
        /* Error starting connection */
        bufferevent_free(bev);
		printf("bufferevent error starting connection\n");
        return;
    }

	printf("done init\n");

}


/*
 * Class:     stanford_mast_reflex_NativeDispatcher
 * Method:    _poll
 * Signature: ()V
 */
JNIEXPORT jint JNICALL Java_stanford_mast_reflex_NativeDispatcher__1poll
  (JNIEnv *env, jobject obj)
{
   	//printf("poll()\n");

	struct worker *worker;
   	int flags = 0;
	
	flags += EVLOOP_NONBLOCK;
	worker = &workers[0]; //FIXME: should be cpu number
   
	num_completions = 0;	
	event_base_loop(worker->base, flags);
	return num_completions;
}



/*
 * Class:     stanford_mast_reflex_NativeDispatcher
 * Method:    _submit_io
 * Signature: (JJIJZ)V
 */
JNIEXPORT jint JNICALL Java_stanford_mast_reflex_NativeDispatcher__1submit_1io
  (JNIEnv *env, jobject obj, jlong addr, jlong lba, jint count, jlong compl_addr, jboolean is_write)
{

	struct worker *worker;
	worker = &workers[0]; //FIXME: should be cpu number
	int pkt_len = 0;
    int	ret = 0;
 
	void* cb_data = reinterpret_cast<io_completion*>(compl_addr);
    void* payload = reinterpret_cast<void*>(addr);


 	binary_header_blk_t *header;
	binary_header_blk_t* pkt = new binary_header_blk_t;
	if (!pkt) {
		printf("error: malloc for response pkt failed\n");
		return -1;
	}

	header = (binary_header_blk_t *)&pkt[0];
	header->magic = sizeof(binary_header_blk_t);
	header->compl_ctx = new completion;
	header->compl_ctx->req_handle = payload; 
	header->compl_ctx->io_compl_addr = cb_data; 
	header->lba = lba;
	header->lba_count = count;

	if (is_write){
		header->opcode = CMD_SET;
		
		//printf("submit_io: write\n");
		// write header to bufferevent
		pkt_len = sizeof(binary_header_blk_t);
		ret = bufferevent_write(worker->bev, (void*) pkt, pkt_len);
		if (ret != 0) {
			printf("send response failed\n");
		}
		// write payload data for write request
		pkt_len = count * SECTOR_SIZE;
		ret = bufferevent_write(worker->bev, (void*) payload, pkt_len);
		if (ret != 0) {
			printf("send response failed\n");
		}
		return 0;
	}

	// else read request...
	//printf("submit_io: read\n");
	header->opcode = CMD_GET;
	pkt_len = sizeof(binary_header_blk_t);
	ret = bufferevent_write(worker->bev, (void*) pkt, pkt_len);
	if (ret != 0) {
		printf("send response failed\n");
	}

  return 0;

}
