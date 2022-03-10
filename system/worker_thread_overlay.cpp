#include "global.h"
#include "message.h"
#include "thread.h"
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "message.h"
#include "timer.h"
#include "chain.h"


#if CONSENSUS == PBFT && Hierarchical

/**
 * upper layer replicas batch the msg to be executed and send it to the lower layer primary replcia
 * primary node is the first node is each cluster.
 * 
**/
void WorkerThread::create_and_send_client_upper(ClientQueryBatch *bmsg, uint64_t tid){
    
    bmsg->init();
    for (uint64_t i = tid; i < tid + get_batch_size(); i++)
    {
        
        TxnManager *txn_man = get_transaction_manager(i, 0);
        BaseQuery *m_query = txn_man->get_query();
        Message *cmsg = Message::create_message((BaseQuery *)m_query, CL_QRY);
        YCSBClientQueryMessage *clqry = (YCSBClientQueryMessage *)(cmsg);
        clqry->copy_from_query(txn_man->get_query());

        bmsg->cqrySet.add(clqry);

    }

    uint64_t start_node = g_node_cnt + g_client_node_cnt;
	for (uint64_t i = 0;i < g_node_id; i++)
		start_node += l_node_cnt[i]; 

	bmsg->sign(start_node);

	// printf("Send to Lower Layer %ld... \n", start_node);
	// fflush(stdout);
	
	vector<uint64_t> dest;
	dest.push_back(start_node);
	msg_queue.enqueue(get_thd_id(),bmsg,dest);
	dest.clear();

}

RC WorkerThread::process_rsp_msg(Message *msg){

    uint64_t ctime = get_sys_clock();

    printf("Replicas process msg %ld ", msg->txn_id);
    fflush(stdout);

	Message *rsp = Message::create_message(CL_RSP);
    ClientResponseMessage *crsp = (ClientResponseMessage *)rsp;
    crsp->init();

	for (uint64_t i = msg->txn_id + 1 - get_batch_size() ;i < msg->txn_id;i++){
    TxnManager *txn = get_transaction_manager(i, 0);
    crsp->copy_from_txn(txn);
}

    TxnManager *txn_man = get_transaction_manager(msg->txn_id, 0);

    // printf("time is %ld", txn_man->client_startts);
    // fflush(stdout);

    crsp->copy_from_txn(txn_man);
    vector<uint64_t> dest;
    dest.push_back(txn_man->client_id);
    msg_queue.enqueue(get_thd_id(), crsp, dest);

    fflush(stdout);

    dest.clear();

    // End the rsp counter.
    INC_STATS(get_thd_id(), time_rsp, get_sys_clock() - ctime);

	return RCOK;

}

#endif



