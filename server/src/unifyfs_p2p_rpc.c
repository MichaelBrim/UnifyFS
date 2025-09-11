/*
 * Copyright (c) 2020, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 *
 * Copyright 2020, UT-Battelle, LLC.
 *
 * LLNL-CODE-741539
 * All rights reserved.
 *
 * This is the license for UnifyFS.
 * For details, see https://github.com/LLNL/UnifyFS.
 * Please read https://github.com/LLNL/UnifyFS/LICENSE for full license text.
 */

// common includes
#include "unifyfs_meta.h"
#include "unifyfs_rpc_util.h"
#include "unifyfs_rpc_types.h"
#include "unifyfs_client_rpcs.h"
#include "unifyfs_server_rpcs.h"

// server includes
#include "unifyfs_global.h"
#include "unifyfs_p2p_rpc.h"
#include "unifyfs_group_rpc.h"


/* arraylist and mutex to track pending remote requests */
extern arraylist_t* pending_remote_requests;
extern ABT_mutex pending_remote_requests_abt_sync;


/*************************************************************************
 * Peer-to-peer RPC helper methods
 *************************************************************************/

/* determine server responsible for maintaining target file's metadata */
int hash_gfid_to_server(int gfid)
{
    return gfid % glb_pmi_size;
}

/* helper method to initialize peer rpc request */
int init_p2p_request(server_rpc_e request_op,
                     int peer_rank,
                     int gfid,
                     p2p_request* preq)
{
    int rc = UNIFYFS_SUCCESS;

    memset((void*)preq, 0, sizeof(p2p_request));

    if (server_exiting) {
        return UNIFYFS_FAILURE;
    }

    preq->req_op = request_op;
    preq->gfid = gfid;

    preq->pending_cond = ABT_COND_NULL;
    preq->pending_sync = ABT_MUTEX_NULL;

    /* get address for specified server rank */
    preq->peer_rank = peer_rank;
    preq->peer = get_margo_server_address(peer_rank);
    if (HG_ADDR_NULL == preq->peer) {
        LOGERR("missing margo address for rank=%d", peer_rank);
        return UNIFYFS_ERROR_MARGO;
    }

    size_t input_sz, output_sz;
    hg_id_t request_hgid = get_rpc_info(request_op, &input_sz, &output_sz);
    rpc_state* rpc = create_rpc_request(request_hgid,
                                        unifyfsd_rpc_context->svr_mid,
                                        preq->peer,
                                        NULL, input_sz,
                                        NULL, output_sz);
    if (NULL == rpc) {
        LOGERR("failed to create p2p request(%p) to server %d",
               preq, peer_rank);
        rc = UNIFYFS_FAILURE;
    } else {
        preq->req_state = rpc;
    }

    return rc;
}

/* helper method to forward peer rpc request */
int forward_p2p_request(p2p_request* preq)
{
    int rc = UNIFYFS_SUCCESS;

    /* call rpc function */
    rc = async_rpc_request(preq->req_state,
                           margo_service_timeout_msec);
    if (rc != UNIFYFS_SUCCESS) {
        LOGERR("failed to forward p2p request(%p)", preq);
    }

    return rc;
}

/* helper method to wait for peer rpc request completion */
int wait_for_p2p_request(p2p_request* preq)
{
    int rc = UNIFYFS_SUCCESS;
    int done = 0;
    int tries = margo_service_retry_count;

    /* finish rpc async call */
    do {
        rc = async_rpc_request_finish(preq->req_state);
        if (rc != UNIFYFS_SUCCESS) {
            if (rc == UNIFYFS_ERROR_TIMEOUT) {
                tries--;
                if (tries <= 0) {
                    done = 1;
                } else {
                    /* generate a new request handle */
                    hg_handle_t new_handle = HG_HANDLE_NULL;
                    rpc_state* rpc = preq->req_state;
                    hg_return_t hret = margo_create(rpc->mid, rpc->maddr,
                                                    rpc->rpc_id, &new_handle);
                    if (hret != HG_SUCCESS) {
                        LOGERR("margo_create() failed - %s",
                               HG_Error_to_string(hret));
                        done = 1;
                    } else {
                        margo_destroy(rpc->handle);
                        rpc->handle = new_handle;
                        rc = forward_p2p_request(preq);
                    }
                }
            }
        } else {
            // some other error encountered
            done = 1;
        }
    } while (!done);
    
    if (rc != UNIFYFS_SUCCESS) {
        LOGERR("failed to finish p2p request(%p)", preq);
    }
    return rc;
}

void cleanup_p2p_request(p2p_request* preq)
{
    /* cleanup pending client reqs */
    if (NULL != preq->pending_client_reqs) {
        /* NOTE: normally, the pending client list should 
         *       already be empty after sending responses */
        client_rpc_req_t* creq;
        int num_pending = arraylist_size(preq->pending_client_reqs);
        for (int i = 0; i < num_pending; i++) {
            creq = (client_rpc_req_t*)
                arraylist_get(preq->pending_client_reqs, i);
            if (creq != NULL) {
                arraylist_remove(preq->pending_client_reqs, i);
                LOGWARN("releasing leftover client rpc request(%p) state",
                        creq);
                release_client_rpc_state(creq);
            }
        }
        arraylist_free(preq->pending_client_reqs);
        preq->pending_client_reqs = NULL;
    }

    /* release other pending state */
    if (ABT_MUTEX_NULL != preq->pending_sync) {
        if (ABT_COND_NULL != preq->pending_cond) {
            int waiters = 0;
            do {
                ABT_mutex_lock(preq->pending_sync);
                waiters = preq->pending_waiters;
                ABT_mutex_unlock(preq->pending_sync);
                if (waiters) {
                    ABT_cond_broadcast(preq->pending_cond);
                }
            } while (waiters > 0);
            ABT_cond_free(&(preq->pending_cond));
        }
        ABT_mutex_free(&(preq->pending_sync));
    }

    /* cleanup p2p rpc state */
    int rc = cleanup_rpc_state(preq->req_state);
    if (rc != UNIFYFS_SUCCESS) {
        LOGERR("failed to cleanup rpc state for p2p request(%p)", preq);
    }
}

int add_pending_remote_request(int peer_rank,
                               int gfid,
                               server_rpc_e op,
                               client_rpc_req_t* client_req,
                               p2p_request** preqp)
{
    int ret, rc;
    int allocated = 0;
    p2p_request* preq = NULL;

    *preqp = NULL;

    ABT_mutex_lock(pending_remote_requests_abt_sync);

    bool have_pending = check_pending_remote_request(peer_rank, gfid,
                                                     op, &preq);
    if (have_pending) {
        LOGDBG("found pending (op=%d) to rank[%d] for gfid=%d - preq(%p)",
               op, peer_rank, gfid, preq);
        ret = UNIFYFS_PENDING;
    } else {
        preq = (p2p_request*) calloc(1, sizeof(p2p_request));
        if (NULL == preq) {
            LOGERR("failed to allocate p2p_request");
            return ENOMEM;
        }
        allocated = 1;
        rc = init_p2p_request(op, peer_rank, gfid, preq);
        if (rc != UNIFYFS_SUCCESS) {
            LOGERR("failed to initialize p2p_request");
            free(preq);
            return rc;
        }
        preq->client_req = client_req;
        LOGDBG("allocated pending (op=%d) to rank[%d] for gfid=%d - preq(%p)",
               op, peer_rank, gfid, preq);
        ret = UNIFYFS_SUCCESS;
    }

    if ((NULL != client_req) && have_pending) {
        /* add client request to pending remote */
        if (NULL == preq->pending_client_reqs) {
            /* create list */
            int max_clients = UNIFYFS_SERVER_MAX_APP_CLIENTS;
            preq->pending_client_reqs = arraylist_create(max_clients);
        }

        /* add current pending client to list */
        rc = arraylist_add(preq->pending_client_reqs, client_req);
        if (-1 == rc) {
            LOGERR("failed to add client req (%p) to pending list",
                   client_req);
        }
    }
    
    if (allocated) {
        if (NULL == client_req) {
            /* not keeping client req list, use condition */
            rc = ABT_mutex_create(&(preq->pending_sync));
            if (ABT_SUCCESS != rc) {
                LOGERR("ABT_mutex_create failed - rc=%d", rc);
                ret = UNIFYFS_ERROR_MARGO;
            } else {
                rc = ABT_cond_create(&(preq->pending_cond));
                if (ABT_SUCCESS != rc) {
                    LOGERR("ABT_cond_create failed - rc=%d", rc);
                    ABT_mutex_free(&(preq->pending_sync));
                    ret = UNIFYFS_ERROR_MARGO;
                }
            }
        }

        /* add new p2p_request to pending remotes list */
        rc = arraylist_add(pending_remote_requests, preq);
        if (rc == -1) {
            LOGERR("failed to add p2p req(%p) to remote_requests arraylist",
                   preq);
            ret = UNIFYFS_FAILURE;
        }
    }

    ABT_mutex_unlock(pending_remote_requests_abt_sync);

    if ((ret == UNIFYFS_SUCCESS) || (ret == UNIFYFS_PENDING)) {
        *preqp = preq;
    } else if (allocated) {
        LOGDBG("an error occurred (ret=%d) - cleaning preq(%p)", ret, preq);
        clear_pending_remote_request(preq);
        cleanup_p2p_request(preq);
        free(preq);
    }
    return ret;
}

bool check_pending_remote_request(int peer_rank,
                                  int gfid,
                                  server_rpc_e op,
                                  p2p_request** preqp)
{
    // NOTE: this method assumes caller has locked sync mutex

    bool is_pending = false;
    p2p_request* pending;

    if (NULL != pending_remote_requests) {
        int num_pending = arraylist_size(pending_remote_requests);
        for (int i = 0; i < num_pending; i++) {
            pending = (p2p_request*) arraylist_get(pending_remote_requests, i);
            if (NULL != pending) {
                if ((pending->peer_rank == peer_rank) &&
                    (pending->gfid == gfid) &&
                    (pending->req_op == op)) {
                    is_pending = true;
                    *preqp = pending;
                    break;
                }
            }
        }
    } else {
        LOGERR("pending_remote_requests is NULL!");
    }

    return is_pending;
}

int clear_pending_remote_request(p2p_request* preq)
{
    int ret = UNIFYFS_FAILURE;
    p2p_request* pending;

    ABT_mutex_lock(pending_remote_requests_abt_sync);
    if (NULL != pending_remote_requests) {
        int num_pending = arraylist_size(pending_remote_requests);
        for (int i = 0; i < num_pending; i++) {
            pending = (p2p_request*) arraylist_get(pending_remote_requests, i);
            if (pending == preq) {
                pending = (p2p_request*)
                    arraylist_remove(pending_remote_requests, i);
                ret = UNIFYFS_SUCCESS;
                break;
            }
        }
    } else {
        LOGERR("pending_remote_requests is NULL!");
        ret = UNIFYFS_FAILURE;
    }
    ABT_mutex_unlock(pending_remote_requests_abt_sync);

    return ret;
}

/* helper method to allocate rpc state for callee */
server_rpc_req_t* allocate_server_rpc_state(server_rpc_e rpc_type,
                                            hg_handle_t handle,
                                            size_t input_sz,
                                            size_t output_sz)
{
    if (server_exiting) {
        return NULL;
    }

    server_rpc_req_t* sreq = (server_rpc_req_t*)
        calloc(1, sizeof(server_rpc_req_t));
    if (NULL != sreq) {
        sreq->req_type = rpc_type;
        void* input = calloc(1, input_sz);
        if (NULL != input) {
            if (HG_HANDLE_NULL != handle) {
                /* if we have a handle, try to get rpc input args */
                hg_return_t hret = margo_get_input(handle, input);
                if (hret != HG_SUCCESS) {
                    LOGERR("margo_get_input() failed - %s",
                           HG_Error_to_string(hret));
                    free(input);
                    free(sreq);
                    sreq = NULL;
                }
            }
            if (NULL != sreq) {
                /* initialize rpc response */
                rpc_state* state = create_rpc_response(handle, input,
                                                       NULL, output_sz);
                if (NULL != state) {
                    sreq->req_state = state;
                } else {
                    if (HG_HANDLE_NULL != handle)
                        margo_free_input(handle, input);
                    free(input);
                    free(sreq);
                    sreq = NULL;
                }
            }
        } else {
            free(sreq);
            sreq = NULL;
        }
    }
    return sreq;
}

void release_server_rpc_state(server_rpc_req_t* sreq)
{
    if (NULL != sreq) {
        if (NULL != sreq->req_state) {
            cleanup_rpc_state(sreq->req_state);
        }
        free(sreq);
    }
}

static void sync_respond_server(server_rpc_req_t* sreq, const char* rpc_name)
{
    rpc_state* rpc = sreq->req_state;
    LOGDBG("responding to the %s server-server rpc(%p)",
           rpc_name, rpc);
    int rc = sync_rpc_response(rpc, margo_service_retry_count);
    if (rc != 0) {
        LOGERR("synchronous %s rpc(%p) response failed (rc=%d)",
               rpc_name, rpc, rc);
    }
    release_server_rpc_state(sreq);
}

#if 0 // MJB-TODO: determine if we need async server-server rpc responses
static int async_respond_server(rpc_state* rpc, const char* rpc_name)
{
    int ret = UNIFYFS_SUCCESS;
    LOGDBG("responding to the %s server-server rpc(%p) asynchronously",
           rpc_name, rpc);
    int rc = async_rpc_response(rpc, margo_service_retry_count);
    if (rc != 0) {
        LOGERR("%s async rpc(%p) response failed (rc=%d)",
               rpc_name, rpc, rc);
        ret = rc;
    }
    return ret;
}

static void async_respond_server_finish(rpc_state* rpc, const char* rpc_name)
{
    LOGDBG("finishing the async %s rpc(%p)", rpc_name, rpc);
    int rc = async_rpc_response_finish(rpc);
    if (rc != 0) {
        LOGERR("%s async rpc(%p) response finish failed (rc=%d)",
               rpc_name, rpc, rc);
    }
    cleanup_rpc_state(rpc);
}
#endif


/*************************************************************************
 * File chunk reads request/response
 *************************************************************************/

/* invokes the server-server chunk read request rpc */
int invoke_chunk_read_request_rpc(int dst_srvr_rank,
                                  server_read_req_t* rdreq,
                                  server_chunk_reads_t* remote_reads)
{
    int num_chunks = remote_reads->num_chunks;
    if (dst_srvr_rank == glb_pmi_rank) {
        // short-circuit for local requests
        return sm_issue_chunk_reads(glb_pmi_rank,
                                    rdreq->app_id,
                                    rdreq->client_id,
                                    rdreq->req_ndx,
                                    num_chunks,
                                    remote_reads->total_sz,
                                    (char*)(remote_reads->reqs));
    }

    int ret = UNIFYFS_SUCCESS;
    hg_return_t hret;
    hg_size_t bulk_sz = (hg_size_t)num_chunks * sizeof(unifyfs_data_chunk_t);

    /* forward request to file owner */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_CHUNK_READ_REQ,
                              dst_srvr_rank, INVALID_GFID, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    chunk_read_request_in_t*  in  = preq.req_state->inputs;
    chunk_read_request_out_t* out = preq.req_state->outputs;

    /* fill input struct */
    in->src_rank        = (int32_t) glb_pmi_rank;
    in->app_id          = (int32_t) rdreq->app_id;
    in->client_id       = (int32_t) rdreq->client_id;
    in->req_id          = (int32_t) rdreq->req_ndx;
    in->num_chks        = (int32_t) num_chunks;
    in->total_data_size = (hg_size_t) remote_reads->total_sz;
    in->bulk_size       = bulk_sz;

    /* register request buffer for bulk remote access */
    void* data_buf = remote_reads->reqs;
    hret = margo_bulk_create(unifyfsd_rpc_context->svr_mid, 1,
                             &data_buf, &bulk_sz,
                             HG_BULK_READ_ONLY, &in->bulk_handle);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s", HG_Error_to_string(hret));
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        LOGDBG("invoking the chunk-read-request rpc function");
        rc = forward_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            LOGERR("forward of chunk-read request rpc to server[%d] failed",
                   dst_srvr_rank);
            margo_bulk_free(in->bulk_handle);
            cleanup_p2p_request(&preq);
            return UNIFYFS_ERROR_MARGO;
        }

        /* wait for request completion */
        rc = wait_for_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
        } else {
            /* decode response */
            ret = (int) out->ret;
            LOGDBG("Got chunk-read response from server[%d] - ret=%d",
                   dst_srvr_rank, ret);
        }

        margo_bulk_free(in->bulk_handle);
    }
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_chunk_read_rpc(server_rpc_req_t* sreq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "chunk_read_request";
    chunk_read_request_in_t* in = sreq->req_state->inputs;
    chunk_read_request_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* issue chunk read requests */
    int src_rank    = (int) in->src_rank;
    int app_id      = (int) in->app_id;
    int client_id   = (int) in->client_id;
    int req_id      = (int) in->req_id;
    int num_chks    = (int) in->num_chks;
    size_t total_sz = (size_t) in->total_data_size;
    size_t bulk_sz = (size_t) in->bulk_size;
    if (bulk_sz) {
        /* allocate and register local target buffer for bulk access */
        void* bulk_buf = pull_margo_bulk(sreq->req_state->handle,
                                         in->bulk_handle, in->bulk_size, NULL);
        if (NULL == bulk_buf) {
            LOGERR("failed to pull chunk reads");
            ret = UNIFYFS_ERROR_MARGO;
        } else {
            LOGDBG("handling chunk read requests from server[%d]: "
                   "req=%d num_chunks=%d data_sz=%zu bulk_sz=%zu",
                   src_rank, req_id, num_chks, total_sz, bulk_sz);

            ret = sm_issue_chunk_reads(src_rank, app_id, client_id,
                                       req_id, num_chks, total_sz,
                                       (char*)bulk_buf);
            free(bulk_buf);
        }
    }

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* handler for server-server chunk read request */
static void chunk_read_request_rpc(hg_handle_t handle)
{
    LOGDBG("chunk_read_request rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_CHUNK_READ_REQ, handle,
                                  sizeof(chunk_read_request_in_t),
                                  sizeof(chunk_read_request_out_t));
    if (NULL == sreq) {
        chunk_read_request_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_chunk_read_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(chunk_read_request_rpc)

/* Respond to chunk read request. Sends a set of read reply
 * headers and corresponding data back to the requesting server.
 * The headers and data are posted as a bulk transfer buffer */
int invoke_chunk_read_response_rpc(server_chunk_reads_t* scr)
{
    /* assume we'll succeed */
    int ret = UNIFYFS_SUCCESS;

    /* rank of destination server */
    int dst_rank = scr->rank;
    assert(dst_rank < (int)glb_num_servers);

    /* forward response to requesting server */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_CHUNK_READ_RESP,
                              dst_rank, INVALID_GFID, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    chunk_read_response_in_t*  in  = preq.req_state->inputs;
    chunk_read_response_out_t* out = preq.req_state->outputs;

    /* get address and size of our response buffer */
    void* data_buf = (void*) scr->resp;
    hg_size_t bulk_sz = scr->total_sz;

    /* register our response buffer for bulk remote read access */
    hg_return_t hret = margo_bulk_create(unifyfsd_rpc_context->svr_mid,
                                         1, &data_buf, &bulk_sz,
                                         HG_BULK_READ_ONLY, &in->bulk_handle);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s", HG_Error_to_string(hret));
        cleanup_p2p_request(&preq);
        return UNIFYFS_ERROR_MARGO;
    }

    /* fill input struct */
    in->src_rank  = (int32_t) glb_pmi_rank;
    in->app_id    = (int32_t) scr->app_id;
    in->client_id = (int32_t) scr->client_id;
    in->req_id    = (int32_t) scr->rdreq_id;
    in->num_chks  = (int32_t) scr->num_chunks;
    in->bulk_size = bulk_sz;

    /* call the read response rpc */
    LOGDBG("invoking the chunk-read-response rpc function");
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        ret = rc;
    } else {
        rc = wait_for_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
        } else {
            /* rpc executed, now decode response */
            ret = (int) out->ret;
            LOGDBG("chunk-read-response rpc to server[%d] - ret=%d",
                   dst_rank, rc);
        }
    }

    /* free resources allocated for executing margo rpc */
    margo_bulk_free(in->bulk_handle);
    cleanup_p2p_request(&preq);

    /* free response data buffer */
    free(data_buf);
    scr->resp = NULL;

    return ret;
}

/* handler for server-server chunk read response */
static void chunk_read_response_rpc(hg_handle_t handle)
{
    int32_t ret = UNIFYFS_SUCCESS;
    chunk_read_response_out_t out;

    /* get input params */
    chunk_read_response_in_t in;
    hg_return_t hret = margo_get_input(handle, &in);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_get_input() failed");
        ret = (int32_t) UNIFYFS_ERROR_MARGO;
    } else {
        /* extract params from input struct */
        int src_rank   = (int) in.src_rank;
        int app_id     = (int) in.app_id;
        int client_id  = (int) in.client_id;
        int req_id     = (int) in.req_id;
        int num_chks   = (int) in.num_chks;
        size_t bulk_sz = (size_t) in.bulk_size;

        LOGDBG("received read response from server[%d] (%d chunks)",
               src_rank, num_chks);

        /* The input parameters specify the info for a bulk transfer
         * buffer on the sending process.  We use that info to pull data
         * from the sender into a local buffer.  This buffer contains
         * the read reply headers and associated read data for requests
         * we had sent earlier. */

        /* pull the remote data via bulk transfer */
        if (0 == bulk_sz) {
            /* sender is trying to send an empty buffer,
             * don't think that should happen unless maybe
             * we had sent a read request list that was empty? */
            LOGERR("empty response buffer");
            ret = (int32_t)EINVAL;
        } else {
            /* allocate a buffer to hold the incoming data */
            char* resp_buf = (char*) pull_margo_bulk(handle,
                                                     in.bulk_handle,
                                                     in.bulk_size,
                                                     NULL);
            if (NULL == resp_buf) {
                /* allocation failed, that's bad */
                LOGERR("failed to pull chunk read responses");
                ret = (int32_t)UNIFYFS_ERROR_MARGO;
            } else {
                LOGDBG("got chunk read responses (%zu bytes)", bulk_sz);

                /* process read replies we just received */
                int rc = rm_post_chunk_read_responses(app_id, client_id,
                                                      src_rank, req_id,
                                                      num_chks, bulk_sz,
                                                      resp_buf);
                if (rc != UNIFYFS_SUCCESS) {
                    LOGERR("failed to handle chunk read responses");
                    ret = rc;
                }
            }
        }
        margo_free_input(handle, &in);
    }

    /* return to caller */
    out.ret = ret;
    hret = margo_respond(handle, &out);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_respond() failed");
    }

    /* free margo resources */
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(chunk_read_response_rpc)


/*************************************************************************
 * File extents metadata update request
 *************************************************************************/

/* Add extents to target file */
int unifyfs_invoke_add_extents_rpc(int gfid,
                                   unsigned int num_extents,
                                   extent_metadata* extents)
{
    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        /* I'm the owner, already did local add */
        return UNIFYFS_SUCCESS;
    }

    /* forward request to file owner */
    int ret = UNIFYFS_SUCCESS;
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_EXTENTS_ADD,
                              owner_rank, gfid, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    add_extents_in_t*  in  = preq.req_state->inputs;
    add_extents_out_t* out = preq.req_state->outputs;

    /* create a margo bulk transfer handle for extents array */
    hg_bulk_t bulk_handle;
    void* buf = (void*) extents;
    size_t buf_sz = (size_t)num_extents * sizeof(extent_metadata);
    hg_return_t hret = margo_bulk_create(unifyfsd_rpc_context->svr_mid,
                                         1, &buf, &buf_sz,
                                         HG_BULK_READ_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s", HG_Error_to_string(hret));
        cleanup_p2p_request(&preq);
        return UNIFYFS_ERROR_MARGO;
    }

    /* fill rpc input struct and forward request */
    in->src_rank    = (int32_t) glb_pmi_rank;
    in->gfid        = (int32_t) gfid;
    in->num_extents = (int32_t) num_extents;
    in->extents     = bulk_handle;
    LOGDBG("forwarding add_extents(gfid=%d) to server[%d]", gfid, owner_rank);
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        ret = rc;
    } else {
        /* wait for request completion */
        rc = wait_for_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
        } else {
            /* get the result of the rpc */
            ret = (int) out->ret;
        }
    }

    margo_bulk_free(bulk_handle);
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_add_extents_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "add_extents";
    add_extents_in_t* in = sreq->req_state->inputs;
    add_extents_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* get input parameters */
    int sender = (int) in->src_rank;
    int gfid = (int) in->gfid;
    size_t num_extents = (size_t) in->num_extents;

    /* allocate and pull bulk extents */
    int ret;
    size_t bulk_sz = num_extents * sizeof(extent_metadata);
    void* buffer = pull_margo_bulk(sreq->req_state->handle,
                                   in->extents, bulk_sz, NULL);
    if (NULL == buffer) {
        LOGERR("failed to pull extents metadata");
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        /* add extents */
        extent_metadata* extents = (extent_metadata*) buffer;
        LOGDBG("adding %zu extents to gfid=%d from server[%d]",
               num_extents, gfid, sender);
        ret = sm_add_extents(gfid, num_extents, extents);
        if (ret) {
            LOGERR("failed to add extents from server[%d] (rc=%d)",
                   sender, ret);
        }
        free(buffer);
    }

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* Add extents rpc handler */
static void add_extents_rpc(hg_handle_t handle)
{
    LOGDBG("add_extents rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_EXTENTS_ADD, handle,
                                  sizeof(add_extents_in_t),
                                  sizeof(add_extents_out_t));
    if (NULL == sreq) {
        add_extents_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_add_extents_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(add_extents_rpc)


/*************************************************************************
 * File extents metadata lookup request
 *************************************************************************/

/* Lookup extent locations for target file */
int unifyfs_find_extent_chunks(unifyfs_fops_ctx_t* ctx,
                               int gfid,
                               unsigned int num_extents,
                               unifyfs_extent_t* extents,
                               unsigned int* num_chunks,
                               unifyfs_data_chunk_t** chunks)
{
    if ((NULL == num_chunks) || (NULL == chunks)) {
        return EINVAL;
    }
    *num_chunks = 0;
    *chunks = NULL;

    int owner_rank = hash_gfid_to_server(gfid);
    int is_owner = (owner_rank == glb_pmi_rank);

    /* do local inode metadata lookup */
    unifyfs_file_attr_t attrs;
    int ret = sm_get_fileattr(gfid, &attrs);
    if (ret == UNIFYFS_SUCCESS) {
        int file_laminated = (attrs.is_shared && attrs.is_laminated);
        if (!(is_owner || use_server_local_extents || file_laminated)) {
            int send_request = 1;
            struct timespec cache_ts, valid_ts;
            ret = unifyfs_inode_get_cache_times(gfid, &cache_ts, &valid_ts);
            if (UNIFYFS_SUCCESS == ret) {
                struct timespec now;
                struct timespec cache_expire = valid_ts;
                cache_expire.tv_sec += UNIFYFS_METADATA_CACHE_SECONDS;
                clock_gettime(CLOCK_REALTIME, &now);
                if (compare_timespec(&now, &cache_expire) <= 0) {
                    /* cached extent metadata is still valid */
                    send_request = 0;
                }
            } else {
                cache_ts = (struct timespec) {0};
            }
            if (send_request) {
                /* send request for updated extents to owner */
                ret = unifyfs_invoke_get_extents_rpc(gfid, &cache_ts);
                if (ret != UNIFYFS_SUCCESS) {
                    LOGERR("request to get extents for gfid=%d failed (ret=%d)",
                        gfid, ret);
                }
            }
        }
    }

    /* now do local lookup */
    int full_coverage = 0;
    ret = sm_find_extents(gfid, (size_t)num_extents, extents,
                          num_chunks, chunks, &full_coverage);
    if (ret) {
        LOGERR("failed to find extents for gfid=%d (ret=%d)",
               gfid, ret);
    } else if (0 == *num_chunks) { /* found no data */
        LOGDBG("local lookup found no matching chunks");
    } else { /* found some chunks */
        if (full_coverage) {
            LOGDBG("local lookup found chunks with full coverage");
        } else {
            LOGDBG("local lookup found chunks with partial coverage");
        }
    }

    return ret;
}

/* Lookup extent locations for target file */
int unifyfs_invoke_get_extents_rpc(int gfid,
                                   struct timespec* timestamp)
{
    if (NULL == timestamp) {
        return EINVAL;
    }

    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        LOGDBG("I am owner (rank=%d) for gfid=%d", owner_rank, gfid);
        return UNIFYFS_SUCCESS;
    }

    int ret = UNIFYFS_SUCCESS;
    p2p_request* preq = NULL;
    int rc = add_pending_remote_request(owner_rank, gfid,
                                        UNIFYFS_SERVER_RPC_EXTENTS_GET,
                                        NULL, &preq);
    if (NULL == preq) {
        LOGERR("failed to add pending remote get_extents");
        return UNIFYFS_FAILURE;
    } else if (rc == UNIFYFS_PENDING) {
        /* wait for completion of pending get_extents request */
        struct timespec pendwait;
        clock_gettime(CLOCK_REALTIME, &pendwait);
        pendwait.tv_sec += UNIFYFS_METADATA_CACHE_SECONDS;
        ABT_mutex_lock(preq->pending_sync);
        preq->pending_waiters++;
        LOGDBG("waiting on pending get_extents condition for preq(%p)", preq);
        rc = ABT_cond_timedwait(preq->pending_cond, preq->pending_sync,
                                &pendwait);
        if (ABT_ERR_COND_TIMEDOUT == rc) {
            LOGERR("wait for pending get_extents timed-out");
            ret = UNIFYFS_ERROR_TIMEOUT;
        } else if (rc) {
            LOGERR("failed to wait on condition (err=%d)", rc);
            ret = UNIFYFS_ERROR_MARGO;
        } else {
            LOGDBG("pending get_extents condition for preq(%p) was signaled",
                   preq);
        }
        preq->pending_waiters--;
        ABT_mutex_unlock(preq->pending_sync);
        return ret;
    } else {
        LOGDBG("added pending get_extents for gfid=%d - preq(%p)", gfid, preq);
    }
        
    assert(preq->req_state != NULL);
    get_extents_in_t*  in  = preq->req_state->inputs;
    get_extents_out_t* out = preq->req_state->outputs;

    /* fill rpc input struct and forward request to owner */
    in->src_rank = (int32_t) glb_pmi_rank;
    in->gfid     = (int32_t) gfid;
    in->src_timestamp = *timestamp;
    rc = forward_p2p_request(preq);
    if (rc != UNIFYFS_SUCCESS) {
        ret = rc;
        goto clear_pending_extents_get;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(preq);
    if (rc != UNIFYFS_SUCCESS) {
        ret = rc;
        goto clear_pending_extents_get;
    }

    /* get the result of the rpc */
    ret = out->ret;
    if (ret == UNIFYFS_SUCCESS) {
        /* get number of extents */
        extent_metadata* em_arr = NULL;
        size_t n_ext = (size_t) out->num_extents;
        struct timespec owner_stamp = (struct timespec) out->owner_timestamp;
        if ((n_ext > 0) && (out->extents != HG_BULK_NULL)) {
            /* get bulk buffer with extent locations */
            size_t buf_sz = n_ext * sizeof(extent_metadata);
            void* buf = pull_margo_bulk(preq->req_state->handle,
                                        out->extents, buf_sz, NULL);
            if (NULL == buf) {
                LOGERR("failed to pull extent metadata array");
                ret = UNIFYFS_ERROR_MARGO;
            } else {
                /* replace local cache with received extents */
                em_arr = (extent_metadata*) buf;
                ret = sm_cache_extents(gfid, n_ext, em_arr, &owner_stamp);
            }
        }
    }

clear_pending_extents_get:
    LOGDBG("clearing pending get_extents for preq(%p) gfid=%d", preq, gfid);
    rc = clear_pending_remote_request(preq);
    if (rc != UNIFYFS_SUCCESS) {
        LOGWARN("failed to clear pending metaget for gfid=%d", gfid);
    }

    int waiters = 0;
    ABT_mutex_lock(preq->pending_sync);
    waiters = preq->pending_waiters;
    ABT_mutex_unlock(preq->pending_sync);
    if (waiters > 1) {
        ABT_cond_broadcast(preq->pending_cond);
    } else if (waiters == 1) {
        ABT_cond_signal(preq->pending_cond);
    }

    cleanup_p2p_request(preq);

    return ret;
}

static struct timespec last_cache_bcast = (struct timespec) {0};

static void process_get_extents_rpc(server_rpc_req_t* sreq)
{
    int ret;
    hg_bulk_t bulk_resp_handle = HG_BULK_NULL;
    size_t num_extents = 0;
    extent_metadata* extents = NULL;
    struct timespec owner_stamp = (struct timespec) {0};

    const char* rpc_name = "get_extents";
    get_extents_in_t* in = sreq->req_state->inputs;
    get_extents_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* get input parameters */
    int sender = (int) in->src_rank;
    int gfid = (int) in->gfid;
    struct timespec src_stamp = (struct timespec) in->src_timestamp;

    int send_extents = 0;
    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        ret = unifyfs_inode_get_extents(gfid, &num_extents, &extents,
                                        &owner_stamp);
        if (ret == UNIFYFS_SUCCESS) {
            /* Compare source timestamp to owner's and do:
             * (a) bcast extents if source is zero, or
             * (b) return extents if source is older than owner
             * (c) return just extent count and timestamp if equal */
            int cmp = compare_timespec(&owner_stamp, &src_stamp);
            if (1 == cmp) {
                /* owner timestamp is newer */
                LOGDBG("owner has newer extents metadata");
                send_extents = 1;
                if (src_stamp.tv_sec == 0) {
                    /* source timestamp is zero, need to broadcast? */
                    send_extents = 0;
                    cmp = compare_timespec(&owner_stamp, &last_cache_bcast);
                    if (0 != cmp) {
                        last_cache_bcast = owner_stamp;
                        LOGDBG("broadcasting extents metadata to cache");
                        ret = unifyfs_invoke_broadcast_extents_cache(gfid);
                        if (UNIFYFS_SUCCESS != ret) {
                            send_extents = 1;
                        }
                    }
                }
            } else if (-1 == cmp) {
                /* source timestamp is newer, which should not happen.
                 * refresh source cache */
                send_extents = 1;
                LOGWARN("source cache timestamp is newer than owner?!?");
            }
            // else, the timestamps are the same, so don't send

            if (send_extents && (num_extents > 0)) {
                /* define a bulk handle to transfer extent_metadata array */
                margo_instance_id mid =
                    margo_hg_handle_get_instance(sreq->req_state->handle);
                assert(mid != MARGO_INSTANCE_NULL);

                hg_size_t buf_sz = num_extents * sizeof(extent_metadata);
                //void* buf = (void*) extents; 
                // MJB TESTING: make a copy to avoid reuse of cache as bulk
                //              across concurrent bcasts
                void* buf = malloc((size_t)buf_sz);
                if (NULL != buf) {
                    memcpy(buf, (void*)extents, (size_t)buf_sz);
                    hg_bulk_t bulk_handle = HG_BULK_NULL;
                    hg_return_t hret = margo_bulk_create(mid, 1, &buf, &buf_sz,
                                                         HG_BULK_READ_ONLY,
                                                         &bulk_handle);
                    if (hret != HG_SUCCESS) {
                        LOGERR("margo_bulk_create() failed - %s",
                               HG_Error_to_string(hret));
                        ret = UNIFYFS_ERROR_MARGO;
                        free(buf);
                    } else {
                        /* set request output bulk for auto-free at cleanup */
                        sreq->req_state->bulk = bulk_handle;
                        sreq->req_state->bulk_buf = buf;
                        bulk_resp_handle = bulk_handle;
                        LOGDBG("returning %zu extents for gfid=%d to rank=%d",
                            num_extents, gfid, sender);
                    }
                }
            }
        }
    } else {
        LOGERR("get_extents request to non-owner rank=%d (gfid=%d, owner=%d)",
               glb_pmi_rank, gfid, owner_rank);
        ret = UNIFYFS_FAILURE;
    }

    out->ret         = (int32_t) ret;
    out->num_extents = (int32_t) num_extents;
    out->extents     = bulk_resp_handle;
    out->owner_timestamp = owner_stamp;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* get extents rpc handler */
static void get_extents_rpc(hg_handle_t handle)
{
    LOGDBG("get_extents rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_EXTENTS_GET, handle,
                                  sizeof(get_extents_in_t),
                                  sizeof(get_extents_out_t));
    if (NULL == sreq) {
        get_extents_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_get_extents_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(get_extents_rpc)


/*************************************************************************
 * File attributes request
 *************************************************************************/

/* Get file attributes for target file */
int unifyfs_invoke_metaget_rpc(unifyfs_fops_ctx_t* ctx,
                               int gfid,
                               unifyfs_file_attr_t* attrs)
{
    assert(NULL != attrs);

    int owner_rank = hash_gfid_to_server(gfid);
    assert(owner_rank != glb_pmi_rank);

    client_rpc_req_t* creq = NULL;
    if (NULL != ctx) {
        /* have a client request that needs the response */
        creq = ctx->client_req;
    }
    p2p_request* preq = NULL;
    int rc = add_pending_remote_request(owner_rank, gfid,
                                        UNIFYFS_SERVER_RPC_METAGET,
                                        creq, &preq);
    if (NULL == preq) {
        LOGERR("failed to create pending remote metaget");
        return UNIFYFS_FAILURE;
    } else if (rc == UNIFYFS_PENDING) {
        return rc;
    }

    assert(preq->req_state != NULL);
    metaget_in_t*  in  = preq->req_state->inputs;
    metaget_out_t* out = preq->req_state->outputs;

    /* fill rpc input struct and forward request to owner */
    int ret = UNIFYFS_SUCCESS;
    in->gfid = (int32_t) gfid;
    rc = forward_p2p_request(preq);
    if (rc != UNIFYFS_SUCCESS) {
        ret = rc;
        goto clear_pending_metaget;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(preq);
    if (rc != UNIFYFS_SUCCESS) {
        ret = rc;
        goto clear_pending_metaget;
    }

    /* get the result of the rpc */
    ret = (int) out->ret;
    if (ret == UNIFYFS_SUCCESS) {
        *attrs = out->attr;
        if (out->attr.filename != NULL) {
            attrs->filename = strdup(out->attr.filename);
        }
        sm_cache_fileattr(gfid, attrs);
    }

clear_pending_metaget:
    LOGDBG("clearing pending metaget for gfid=%d", gfid);
    rc = clear_pending_remote_request(preq);
    if (rc != UNIFYFS_SUCCESS) {
        LOGWARN("failed to clear pending metaget for gfid=%d", gfid);
    }

    if (NULL != preq->pending_client_reqs) {
        client_rpc_req_t* creq;
        unifyfs_metaget_out_t* mout;
        unifyfs_filesize_out_t* fout;
        const char* rpc_name;
        const char* metaget_rpc = "unifyfs_metaget";
        const char* filesize_rpc = "unifyfs_filesize";
        const char* unknown_rpc = "!!UNKNOWN-CLIENT-RPC!!";
        int num_pending = arraylist_size(preq->pending_client_reqs);
        // start async responses
        for (int i = 0; i < num_pending; i++) {
            creq = (client_rpc_req_t*)
                arraylist_get(preq->pending_client_reqs, i);
            if (NULL != creq) {
                rpc_name = NULL;
                if (creq->req_type == UNIFYFS_CLIENT_RPC_METAGET) {
                    mout = creq->req_state->outputs;
                    mout->ret = (int32_t) ret;
                    mout->attr = *attrs;
                    rpc_name = metaget_rpc;
                } else if (creq->req_type == UNIFYFS_CLIENT_RPC_FILESIZE) {
                    fout = creq->req_state->outputs;
                    fout->ret = (int32_t) ret;
                    fout->filesize = (hg_size_t) attrs->size;
                    rpc_name = filesize_rpc;
                } else {
                    LOGWARN("unexpected client req type %d", creq->req_type);
                    rpc_name = unknown_rpc;
                }
                rc = async_respond_client(creq, rpc_name);
                if (rc != UNIFYFS_SUCCESS) {
                    LOGERR("failed async response to client req(%p)",
                           creq);
                }
            }
        }
        // finish async responses
        for (int i = 0; i < num_pending; i++) {
            creq = (client_rpc_req_t*)
                arraylist_remove(preq->pending_client_reqs, i);
            if (NULL != creq) {
                rpc_name = NULL;
                if (creq->req_type == UNIFYFS_CLIENT_RPC_METAGET) {
                    rpc_name = metaget_rpc;
                } else if (creq->req_type == UNIFYFS_CLIENT_RPC_FILESIZE) {
                    rpc_name = filesize_rpc;
                } else {
                    rpc_name = unknown_rpc;
                }
                /* note: the following will release creq allocated state */
                async_respond_client_finish(creq, rpc_name);
            }
        }
    }

    cleanup_p2p_request(preq);

    return ret;
}

static void process_metaget_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "metaget";
    metaget_in_t* in = sreq->req_state->inputs;
    metaget_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* get metadata for target file */
    unifyfs_file_attr_t attrs;
    unifyfs_file_attr_set_invalid(&attrs);
    int gfid = (int) in->gfid;
    int ret = sm_get_fileattr(gfid, &attrs);

    out->ret = (int32_t) ret;
    out->attr = attrs;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* Metaget rpc handler */
static void metaget_rpc(hg_handle_t handle)
{
    LOGDBG("metaget rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_METAGET, handle,
                                  sizeof(metaget_in_t),
                                  sizeof(metaget_out_t));
    if (NULL == sreq) {
        metaget_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_metaget_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(metaget_rpc)


/*************************************************************************
 * File attributes update request
 *************************************************************************/

/* Set metadata for target file */
int unifyfs_invoke_metaset_rpc(unifyfs_fops_ctx_t* ctx,
                               int gfid,
                               int attr_op,
                               unifyfs_file_attr_t* attrs)
{
    if (NULL == attrs) {
        return EINVAL;
    }

    int ret = sm_set_fileattr(gfid, attr_op, attrs);
    if (ret != UNIFYFS_SUCCESS) {
        return ret;
    }

    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        /* I'm the owner, return local result */
        return ret;
    }

    /* forward request to file owner */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_METASET,
                              owner_rank, gfid, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    metaset_in_t*  in  = preq.req_state->inputs;
    metaset_out_t* out = preq.req_state->outputs;

    /* fill rpc input struct and forward request */
    in->gfid   = (int32_t) gfid;
    in->fileop = (int32_t) attr_op;
    in->attr   = *attrs;
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* get the result of the rpc */
    ret = (int) out->ret;
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_metaset_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "metaset";
    metaset_in_t* in = sreq->req_state->inputs;
    metaset_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* update target file metadata */
    unifyfs_file_attr_t* attrs = &(in->attr);
    int gfid = (int) in->gfid;
    int attr_op = (int) in->fileop;
    int ret = sm_set_fileattr(gfid, attr_op, attrs);

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* Metaset rpc handler */
static void metaset_rpc(hg_handle_t handle)
{
    LOGDBG("metaset rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_METASET, handle,
                                  sizeof(metaset_in_t),
                                  sizeof(metaset_out_t));
    if (NULL == sreq) {
        metaset_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_metaset_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(metaset_rpc)


/*************************************************************************
 * File lamination request
 *************************************************************************/

/*  Laminate the target file */
int unifyfs_invoke_laminate_rpc(unifyfs_fops_ctx_t* ctx,
                                int gfid)
{
    int ret;
    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        /* I'm the owner, do local inode metadata update */
        return sm_laminate(gfid);
    }

    /* forward request to file owner */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_LAMINATE,
                              owner_rank, gfid, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    laminate_in_t*  in  = preq.req_state->inputs;
    laminate_out_t* out = preq.req_state->outputs;

    /* fill rpc input struct and forward request */
    in->gfid = (int32_t) gfid;
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* get the output of the rpc */
    ret = (int) out->ret;
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_laminate_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "laminate";
    laminate_in_t* in = sreq->req_state->inputs;
    laminate_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* do file lamination */
    int gfid = (int) in->gfid;
    int ret = sm_laminate(gfid);

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* Laminate rpc handler */
static void laminate_rpc(hg_handle_t handle)
{
    LOGDBG("laminate rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_LAMINATE, handle,
                                  sizeof(laminate_in_t),
                                  sizeof(laminate_out_t));
    if (NULL == sreq) {
        laminate_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_laminate_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(laminate_rpc)


/*************************************************************************
 * Read a remote chunk into local extent buffer at given offset
 *************************************************************************/

/* Read a remote chunk */
int unifyfs_invoke_read_chunk_rpc(unifyfs_data_chunk_t* chunk,
                                  size_t bulk_offset,
                                  hg_bulk_t bulk_extent,
                                  size_t* bytes_read)
{
    /* forward request to file owner */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_READ_CHUNK,
                              chunk->log_server, chunk->gfid, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    read_chunk_in_t*  in  = preq.req_state->inputs;
    read_chunk_out_t* out = preq.req_state->outputs;

    /* fill rpc input struct and forward request */
    in->chunk       = *chunk;
    in->bulk_offset = (hg_size_t) bulk_offset;
    in->bulk_handle = bulk_extent;
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* get the result of the rpc */
    int ret = (int) out->ret;
    if (NULL != bytes_read) {
        *bytes_read = out->bytes_read;
    }
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_read_chunk_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "read_chunk";
    read_chunk_in_t* in = sreq->req_state->inputs;
    read_chunk_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    size_t nbytes = 0;
    unifyfs_data_chunk_t* chunk = &(in->chunk);

    int rc;
    void* readbuf = calloc(1, chunk->length);
    if (NULL == readbuf) {
        LOGERR("failed to allocate local read buffer");
        rc = ENOMEM;
    } else {
        rc = sm_read_chunk(chunk, readbuf, &nbytes);
        if (rc == UNIFYFS_SUCCESS) {
            /* push data into remote bulk buffer */
            rc = push_margo_bulk(sreq->req_state->handle,
                                 in->bulk_handle,
                                 in->bulk_offset,
                                 (hg_size_t) nbytes,
                                 readbuf);
            if (rc != UNIFYFS_SUCCESS) {
                LOGERR("failed to push chunk data to remote bulk (nbytes=%zu)",
                       nbytes);
                nbytes = 0;
            }
        }
        free(readbuf);
    }

    out->ret = (int32_t) rc;
    out->bytes_read = (hg_size_t) nbytes;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* read chunk rpc handler */
static void read_chunk_rpc(hg_handle_t handle)
{
    LOGDBG("read_chunk rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_READ_CHUNK, handle,
                                  sizeof(read_chunk_in_t),
                                  sizeof(read_chunk_out_t));
    if (NULL == sreq) {
        read_chunk_out_t out;
        out.ret = (int32_t) ENOMEM;
        out.bytes_read = 0;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_read_chunk_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(read_chunk_rpc)


/*************************************************************************
 * File transfer request
 *************************************************************************/

/* Transfer the target file */
int unifyfs_invoke_transfer_rpc(int client_app,
                                int client_id,
                                int transfer_id,
                                int gfid,
                                int transfer_mode,
                                const char* dest_file)
{
    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        return sm_transfer(glb_pmi_rank, client_app, client_id, transfer_id,
                           gfid, transfer_mode, dest_file, NULL);
    }

    /* forward request to file owner */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_TRANSFER,
                              owner_rank, gfid, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    transfer_in_t*  in  = preq.req_state->inputs;
    transfer_out_t* out = preq.req_state->outputs;

    /* fill rpc input struct and forward request */
    in->src_rank    = (int32_t) glb_pmi_rank;
    in->client_app  = (int32_t) client_app;
    in->client_id   = (int32_t) client_id;
    in->transfer_id = (int32_t) transfer_id;
    in->gfid        = (int32_t) gfid;
    in->mode        = (int32_t) transfer_mode;
    in->dst_file    = (hg_const_string_t) dest_file;
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* get the result of the rpc */
    int ret = (int) out->ret;
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_transfer_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "transfer";
    transfer_in_t* in = sreq->req_state->inputs;
    transfer_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));
 
    /* get target file and requested file size */
    int src_rank      = (int) in->src_rank;
    int client_app    = (int) in->client_app;
    int client_id     = (int) in->client_id;
    int transfer_id   = (int) in->transfer_id;
    int gfid          = (int) in->gfid;
    int transfer_mode = (int) in->mode;
    char* dest_file   = strdup(in->dst_file);

    /* do file transfer */
    int ret = sm_transfer(src_rank, client_app, client_id, transfer_id,
                          gfid, transfer_mode, dest_file, NULL);
    free(dest_file);

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* Transfer rpc handler */
static void transfer_rpc(hg_handle_t handle)
{
    LOGDBG("transfer rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_TRANSFER, handle,
                                  sizeof(transfer_in_t),
                                  sizeof(transfer_out_t));
    if (NULL == sreq) {
        transfer_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_transfer_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(transfer_rpc)


/*************************************************************************
 * File truncation request
 *************************************************************************/

/* Truncate the target file */
int unifyfs_invoke_truncate_rpc(unifyfs_fops_ctx_t* ctx,
                                int gfid,
                                size_t filesize)
{
    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        return sm_truncate(gfid, filesize);
    }

    /* forward request to file owner */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_TRUNCATE,
                              owner_rank, gfid, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    truncate_in_t*  in  = preq.req_state->inputs;
    truncate_out_t* out = preq.req_state->outputs;

    /* fill rpc input struct and forward request */
    in->gfid     = (int32_t) gfid;
    in->filesize = (hg_size_t) filesize;
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* get the output of the rpc */
    int ret = (int) out->ret;
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_truncate_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "truncate";
    truncate_in_t* in = sreq->req_state->inputs;
    truncate_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* do file truncation */
    size_t fsize = (size_t) in->filesize;
    int gfid = (int) in->gfid;
    int ret = sm_truncate(gfid, fsize);

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* Truncate rpc handler */
static void truncate_rpc(hg_handle_t handle)
{
    LOGDBG("truncate rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_TRUNCATE, handle,
                                  sizeof(truncate_in_t),
                                  sizeof(truncate_out_t));
    if (NULL == sreq) {
        truncate_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_truncate_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(truncate_rpc)

/*************************************************************************
 * Server pid report
 *************************************************************************/

int unifyfs_invoke_server_pid_rpc(void)
{
    /* forward pid to server rank 0 */
    p2p_request preq;
    int rc = init_p2p_request(UNIFYFS_SERVER_RPC_SERVER_PID,
                              0, INVALID_GFID, &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }
    assert(preq.req_state != NULL);
    server_pid_in_t*  in  = preq.req_state->inputs;
    server_pid_out_t* out = preq.req_state->outputs;

    /* fill rpc input struct and forward request */
    in->rank = glb_pmi_rank;
    in->pid = server_pid;
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* wait for request completion */
    rc = wait_for_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* get the output of the rpc */
    int ret = (int) out->ret;
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_server_pid_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "server_pid";
    server_pid_in_t* in = sreq->req_state->inputs;
    server_pid_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    int rank = (int) in->rank;
    int pid  = (int) in->pid;
    int ret = unifyfs_report_server_pid(rank, pid);

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

static void server_pid_rpc(hg_handle_t handle)
{
    LOGDBG("server pid report rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_SERVER_PID, handle,
                                  sizeof(server_pid_in_t),
                                  sizeof(server_pid_out_t));
    if (NULL == sreq) {
        server_pid_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_server_pid_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(server_pid_rpc)
