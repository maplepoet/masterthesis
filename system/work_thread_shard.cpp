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


#if CONSENSUS == PBFT && Shard

// primary nodes
RC WorkerThread::process_client_batch(Message *msg){

   ClientQueryBatch *clbtch = (ClientQueryBatch *)msg;

	// Authenticate the client signature.
	validate_msg(clbtch);

#if VIEW_CHANGES
	// If message forwarded to the non-primary.
	if (g_node_id != get_current_view(get_thd_id()))
	{
		client_query_check(clbtch);
		cout << "returning...   " << get_current_view(get_thd_id()) << endl;
		return RCOK;
	}

	// Partial failure of Primary 0.
	fail_primary(msg, 10 * BILLION);
#endif

	// Initialize all transaction mangers and Send BatchRequests message.
	create_and_send_batchreq(clbtch, clbtch->txn_id);

	return RCOK;

}


// non-primary nodes
RC WorkerThread::process_batch(Message *msg)
{   
    uint64_t cntime = get_sys_clock();

    BatchRequests *breq = (BatchRequests *)msg;

    // Assert that only a non-primary replica has received this message.
	assert(g_node_id != get_current_view(get_thd_id()));

	// Check if the message is valid.
	validate_msg(breq);

#if VIEW_CHANGES
	// Store the batch as it could be needed during view changes.
	store_batch_msg(breq);
#endif

	// Allocate transaction managers for all the transactions in the batch.
	set_txn_man_fields(breq, 0);

#if TIMER_ON
	// The timer for this client batch stores the hash of last request.
	add_timer(breq, txn_man->get_hash());
#endif

	// Storing the BatchRequests message.
	txn_man->set_primarybatch(breq);

	// Send Prepare messages.
	txn_man->send_pbft_prep_msgs();

    // End the counter for pre-prepare phase as prepare phase starts next.
	double timepre = get_sys_clock() - cntime;
	INC_STATS(get_thd_id(), time_pre_prepare, timepre);

	// Only when BatchRequests message comes after some Prepare message.
	for (uint64_t i = 0; i < txn_man->info_prepare.size(); i++)
	{
		// Decrement.
		uint64_t num_prep = txn_man->decr_prep_rsp_cnt();
		if (num_prep == 0)
		{
			txn_man->set_prepared();
			break;
		}
	}

    // If enough Prepare messages have already arrived.
	if (txn_man->is_prepared())
	{
		// Send Commit messages.
		txn_man->send_pbft_commit_msgs();

		double timeprep = get_sys_clock() - txn_man->txn_stats.time_start_prepare - timepre;
		INC_STATS(get_thd_id(), time_prepare, timeprep);
		double timediff = get_sys_clock() - cntime;

		// Check if any Commit messages arrived before this BatchRequests message.
		for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
		{
			uint64_t num_comm = txn_man->decr_commit_rsp_cnt();
			if (num_comm == 0)
			{
				txn_man->set_committed();
				break;
			}
		}

        // If enough Commit messages have already arrived.
		if (txn_man->is_committed())
		{
#if TIMER_ON
			// End the timer for this client batch.
			remove_timer(txn_man->hash);
#endif
			// Proceed to executing this batch of transactions.
			send_execute_msg();

			// End the commit counter.
			INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit - timediff);
		}
	}
	else
	{
		// Although batch has not prepared, still some commit messages could have arrived.
		for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
		{
			txn_man->decr_commit_rsp_cnt();
		}
	}

	// Release this txn_man for other threads to use.
	bool ready = txn_man->set_ready();
	assert(ready);

	// UnSetting the ready for the txn id representing this batch.
	txn_man = get_transaction_manager(msg->txn_id, 0);
	unset_ready_txn(txn_man);

    return RCOK;
}


RC WorkerThread::process_pbft_prep_msg(Message *msg)
{

	// Start the counter for prepare phase.
	if (txn_man->prep_rsp_cnt == 2 * g_min_invalid_nodes)
	{
		txn_man->txn_stats.time_start_prepare = get_sys_clock();
	}

	// Check if the incoming message is valid.
	PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;
	validate_msg(pmsg);

	// Check if sufficient number of Prepare messages have arrived.
	if (prepared(pmsg))
	{
		// Send Commit messages.
		txn_man->send_pbft_commit_msgs();

		// End the prepare counter.
		INC_STATS(get_thd_id(), time_prepare, get_sys_clock() - txn_man->txn_stats.time_start_prepare);
	}

	return RCOK;
}

// After Node A receives the messages from the shards,  it sends the response messages to the Clients.
RC WorkerThread::process_response_msg(Message *msg){
    return RCOK;
}


RC WorkerThread::send_forward_msg(Message *msg){

    // find the shard;
    uint64_t _dest = g_node_id + g_node_cnt;
    
    // find the next shard;

    Message *fmsg = Message::create_message(FORWARD_REQ);
    ForwardRequests *frqry = (ForwardRequests *)fmsg;

    frqry->init();
    uint64_t tid = msg->txn_id;

    for (uint64_t i = tid; i < tid + get_batch_size(); i++){
        TxnManager *txn_man = get_transaction_manager(i,0);
        BaseQuery *m_query = txn_man->get_query();
        Message *cmsg = Message::create_message((BaseQuery *)m_query, CL_QRY);
        YCSBClientQueryMessage *clqry = (YCSBClientQueryMessage *)(cmsg);
        clqry->copy_from_query(txn_man->get_query());
        frqry->cqrySet.add(clqry);
    }

    
    vector<uint64_t> dest;
    dest.push_back(_dest);
    msg_queue.enqueue(get_thd_id(), frqry, dest);
    dest.clear();

    return RCOK;
}


RC WorkerThread::process_forward_msg(Message *msg){

    if (g_node_id>=0 && g_node_id<g_node_cnt){
        send_response_msg(msg);
        return RCOK;
    }

    ForwardRequests *frqry = (ForwardRequests*)msg;
    if (g_node_id == 4)
        create_and_send_batchreq_shard(frqry, frqry->txn_id);

    return RCOK;

}

void WorkerThread::create_and_send_batchreq_shard(ForwardRequests *msg, uint64_t tid){

    // Creating a new BatchRequests Message.
    Message *bmsg = Message::create_message(BATCH_REQ);
    BatchRequests *breq = (BatchRequests *)bmsg;
    breq->init(get_thd_id());

    // Starting index for this batch of transactions.
    next_set = tid;

    // String of transactions in a batch to generate hash.
    string batchStr;

    // Allocate transaction manager for all the requests in batch.
    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        uint64_t txn_id = get_next_txn_id() + i;

        //cout << "Txn: " << txn_id << " :: Thd: " << get_thd_id() << "\n";
        //fflush(stdout);
        txn_man = get_transaction_manager(txn_id, 0);

        // Unset this txn man so that no other thread can concurrently use.
        unset_ready_txn(txn_man);

        txn_man->register_thread(this);
        txn_man->return_id = msg->return_node;
        txn_man->client_id = msg->return_node;

        // Fields that need to updated according to the specific algorithm.
        algorithm_specific_update(msg, i);

        init_txn_man(msg->cqrySet[i]);

        // Append string representation of this txn.
        batchStr += msg->cqrySet[i]->getString();

        // Setting up data for BatchRequests Message.
        breq->copy_from_txn(txn_man, msg->cqrySet[i]);

        // Reset this txn manager.
        bool ready = txn_man->set_ready();
        assert(ready);
    }

    // Now we need to unset the txn_man again for the last txn of batch.
    unset_ready_txn(txn_man);

    // Generating the hash representing the whole batch in last txn man.
    txn_man->set_hash(calculateHash(batchStr));
    txn_man->hashSize = txn_man->hash.length();

    breq->copy_from_txn(txn_man);

    // Storing the BatchRequests message.
    txn_man->set_primarybatch(breq);

    vector<uint64_t> dest;

    // Storing all the signatures.

    //     printf("internal layer send pre-pare. \n ");
    //     fflush(stdout);
        
    for (uint64_t i = 4; i < 8; i++)
    {
#if GBFT

        if (!is_in_same_cluster(i, g_node_id))
        {
            continue;
        }
#endif
        if (i == g_node_id)
        {
            continue;
        }
        dest.push_back(i);
    }   
    msg_queue.enqueue(get_thd_id(), breq, dest);
    dest.clear();

}


void WorkerThread::send_response_msg(Message *msg){

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

}


uint64_t find_shard(uint64_t g_node_id){
    uint64_t shard_id = 0;

}


#endif





