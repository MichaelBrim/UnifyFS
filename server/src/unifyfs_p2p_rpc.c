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
#include "unifyfs_rpc_util.h"
#include "unifyfs_rpc_types.h"
#include "unifyfs_server_rpcs.h"

// server includes
#include "unifyfs_global.h"
#include "unifyfs_p2p_rpc.h"
#include "unifyfs_group_rpc.h"

/*************************************************************************
 * Peer-to-peer RPC helper methods
 *************************************************************************/

/* determine server responsible for maintaining target file's metadata */
int hash_gfid_to_server(int gfid)
{
    return gfid % glb_pmi_size;
}

/* helper method to initialize peer request rpc handle */
int init_p2p_request(hg_id_t request_hgid,
                     int peer_rank,
                     void* input_args,
                     void* output_args,
                     p2p_request* preq)
{
    int rc = UNIFYFS_SUCCESS;

    /* get address for specified server rank */
    preq->peer_rank = peer_rank;
    preq->peer = get_margo_server_address(peer_rank);
    if (HG_ADDR_NULL == preq->peer) {
        LOGERR("missing margo address for rank=%d", peer_rank);
        return UNIFYFS_ERROR_MARGO;
    }

    rpc_state* rpc = create_rpc_request(request_hgid,
                                        unifyfsd_rpc_context->svr_mid,
                                        preq->peer,
                                        input_args, 0,
                                        output_args, 0);
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

    /* finish rpc async call */
    rc = async_rpc_request_finish(preq->req_state);
    if (rc != UNIFYFS_SUCCESS) {
        LOGERR("failed to finish p2p request(%p)", preq);
    }

    return rc;
}

void cleanup_p2p_request(p2p_request* preq)
{
    /* cleanup rpc state */
    int rc = cleanup_rpc_state(preq->req_state);
    if (rc != UNIFYFS_SUCCESS) {
        LOGERR("failed to cleanup rpc state for p2p request(%p)", preq);
    }
}

server_rpc_req_t* allocate_server_rpc_state(server_rpc_e rpc_type,
                                            hg_handle_t handle,
                                            size_t input_sz,
                                            size_t output_sz)
{
    server_rpc_req_t* sreq = (server_rpc_req_t*)
        calloc(1, sizeof(server_rpc_req_t));
    if (NULL != sreq) {
        sreq->req_type = rpc_type;
        void* input = calloc(1, input_sz);
        if (NULL != input) {
            if (HG_HANDLE_NULL != handle) {
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

#if 0 // TODO: determine if we need async server-server rpc responses
static int async_respond_client(rpc_state* rpc, const char* rpc_name)
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

static void async_respond_finish(rpc_state* rpc, const char* rpc_name)
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
    hg_size_t bulk_sz = (hg_size_t)num_chunks * sizeof(chunk_read_req_t);

    /* forward request to file owner */
    chunk_read_request_in_t in;
    chunk_read_request_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.chunk_read_request_id;
    int rc = init_p2p_request(req_hgid, dst_srvr_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* fill input struct */
    in.src_rank        = (int32_t) glb_pmi_rank;
    in.app_id          = (int32_t) rdreq->app_id;
    in.client_id       = (int32_t) rdreq->client_id;
    in.req_id          = (int32_t) rdreq->req_ndx;
    in.num_chks        = (int32_t) num_chunks;
    in.total_data_size = (hg_size_t) remote_reads->total_sz;
    in.bulk_size       = bulk_sz;

    /* register request buffer for bulk remote access */
    void* data_buf = remote_reads->reqs;
    hret = margo_bulk_create(unifyfsd_rpc_context->svr_mid, 1,
                             &data_buf, &bulk_sz,
                             HG_BULK_READ_ONLY, &in.bulk_handle);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s", HG_Error_to_string(hret));
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        LOGDBG("invoking the chunk-read-request rpc function");
        rc = forward_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            LOGERR("forward of chunk-read request rpc to server[%d] failed",
                   dst_srvr_rank);
            margo_bulk_free(in.bulk_handle);
            cleanup_p2p_request(&preq);
            return UNIFYFS_ERROR_MARGO;
        }

        /* wait for request completion */
        rc = wait_for_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
        } else {
            /* decode response */
            ret = (int) out.ret;
            LOGDBG("Got chunk-read response from server[%d] - ret=%d",
                   dst_srvr_rank, ret);
        }

        margo_bulk_free(in.bulk_handle);
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
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_CHUNK_READ, handle,
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
    chunk_read_response_in_t in;
    chunk_read_response_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.chunk_read_response_id;
    int rc = init_p2p_request(req_hgid, dst_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* get address and size of our response buffer */
    void* data_buf = (void*) scr->resp;
    hg_size_t bulk_sz = scr->total_sz;

    /* register our response buffer for bulk remote read access */
    hg_return_t hret = margo_bulk_create(unifyfsd_rpc_context->svr_mid,
                                         1, &data_buf, &bulk_sz,
                                         HG_BULK_READ_ONLY, &in.bulk_handle);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s", HG_Error_to_string(hret));
        cleanup_p2p_request(&preq);
        return UNIFYFS_ERROR_MARGO;
    }

    /* fill input struct */
    in.src_rank  = (int32_t) glb_pmi_rank;
    in.app_id    = (int32_t) scr->app_id;
    in.client_id = (int32_t) scr->client_id;
    in.req_id    = (int32_t) scr->rdreq_id;
    in.num_chks  = (int32_t) scr->num_chunks;
    in.bulk_size = bulk_sz;

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
            ret = (int) out.ret;
            LOGDBG("chunk-read-response rpc to server[%d] - ret=%d",
                   dst_rank, rc);
        }
    }

    /* free resources allocated for executing margo rpc */
    margo_bulk_free(in.bulk_handle);
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
        int src_rank   = (int)in.src_rank;
        int app_id     = (int)in.app_id;
        int client_id  = (int)in.client_id;
        int req_id     = (int)in.req_id;
        int num_chks   = (int)in.num_chks;
        size_t bulk_sz = (size_t)in.bulk_size;

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
    add_extents_in_t in;
    add_extents_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.extent_add_id;
    int rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

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
    in.src_rank    = (int32_t) glb_pmi_rank;
    in.gfid        = (int32_t) gfid;
    in.num_extents = (int32_t) num_extents;
    in.extents     = bulk_handle;
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
            ret = (int) out.ret;
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
int unifyfs_invoke_find_extents_rpc(int gfid,
                                    unsigned int num_extents,
                                    unifyfs_extent_t* extents,
                                    unsigned int* num_chunks,
                                    chunk_read_req_t** chunks)
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
        if (is_owner || use_server_local_extents || file_laminated) {
            /* try local lookup */
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
            if (is_owner || file_laminated || full_coverage) {
                return ret;
            }
            /* else, fall through to owner lookup */
            if (*num_chunks > 0) {
                /* release local results */
                *num_chunks = 0;
                free(*chunks);
                *chunks = NULL;
            }
        }
    }

    /* forward request to file owner */
    find_extents_in_t in;
    find_extents_out_t out;
    p2p_request preq;
    margo_instance_id mid = unifyfsd_rpc_context->svr_mid;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.extent_lookup_id;
    int rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* create a margo bulk transfer handle for extents array */
    hg_bulk_t bulk_req_handle;
    void* buf = (void*) extents;
    size_t buf_sz = (size_t)num_extents * sizeof(unifyfs_extent_t);
    hg_return_t hret = margo_bulk_create(mid, 1, &buf, &buf_sz,
                                         HG_BULK_READ_ONLY, &bulk_req_handle);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s", HG_Error_to_string(hret));
        cleanup_p2p_request(&preq);
        return UNIFYFS_ERROR_MARGO;
    }

    /* fill rpc input struct and forward request */
    in.src_rank    = (int32_t) glb_pmi_rank;
    in.gfid        = (int32_t) gfid;
    in.num_extents = (int32_t) num_extents;
    in.extents     = bulk_req_handle;
    rc = forward_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }
    margo_bulk_free(bulk_req_handle);

    /* wait for request completion */
    rc = wait_for_p2p_request(&preq);
    if (rc != UNIFYFS_SUCCESS) {
        cleanup_p2p_request(&preq);
        return rc;
    }

    /* get the result of the rpc */
    ret = out.ret;
    if (ret == UNIFYFS_SUCCESS) {
        /* get number of chunks */
        unsigned int n_chks = (unsigned int) out.num_locations;
        if (n_chks > 0) {
            /* get bulk buffer with chunk locations */
            buf_sz = (size_t)n_chks * sizeof(chunk_read_req_t);
            buf = pull_margo_bulk(preq.req_state->handle, out.locations,
                                  buf_sz, NULL);
            if (NULL == buf) {
                LOGERR("failed to pull chunk locations");
                ret = UNIFYFS_ERROR_MARGO;
            } else {
                /* lookup requested extents */
                LOGDBG("received %u chunk locations for gfid=%d",
                       n_chks, gfid);
                *chunks = (chunk_read_req_t*) buf;
                *num_chunks = (unsigned int) n_chks;
            }
        }
    }
    cleanup_p2p_request(&preq);

    return ret;
}

static void process_find_extents_rpc(server_rpc_req_t* sreq)
{
    int ret;
    hg_bulk_t bulk_resp_handle = HG_BULK_NULL;
    unsigned int num_chunks = 0;
    
    const char* rpc_name = "find_extents";
    find_extents_in_t* in = sreq->req_state->inputs;
    find_extents_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* get input parameters */
    int sender = (int) in->src_rank;
    int gfid = (int) in->gfid;
    size_t num_extents = (size_t) in->num_extents;
    
    /* allocate and pull bulk extents */
    size_t bulk_sz = num_extents * sizeof(unifyfs_extent_t);
    void* bulk_buf = pull_margo_bulk(sreq->req_state->handle,
                                   in->extents, bulk_sz, NULL);
    if (NULL == bulk_buf) {
        LOGERR("failed to pull extents");
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        unifyfs_extent_t* extents = (unifyfs_extent_t*) bulk_buf;
        LOGDBG("received %zu extent lookups for gfid=%d from server[%d]",
               num_extents, gfid, sender);

        /* find chunks for given extents */
        int full_coverage = 0;
        
        chunk_read_req_t* chunk_locs = NULL;
        ret = sm_find_extents(gfid, num_extents, extents,
                              &num_chunks, &chunk_locs, &full_coverage);
        if (ret == UNIFYFS_SUCCESS) {
            /* define a bulk handle to transfer chunk address info */
            if (num_chunks > 0) {
                margo_instance_id mid =
                    margo_hg_handle_get_instance(sreq->req_state->handle);
                assert(mid != MARGO_INSTANCE_NULL);

                void* buf = (void*) chunk_locs;
                size_t buf_sz = (size_t)num_chunks * sizeof(chunk_read_req_t);
                hg_return_t hret = margo_bulk_create(mid, 1, &buf, &buf_sz,
                                                     HG_BULK_READ_ONLY,
                                                     &bulk_resp_handle);
                if (hret != HG_SUCCESS) {
                    LOGERR("margo_bulk_create() failed - %s",
                           HG_Error_to_string(hret));
                    ret = UNIFYFS_ERROR_MARGO;
                } else {
                    /* set request output bulk for auto-free at cleanup */
                    sreq->req_state->bulk = bulk_resp_handle;
                }
            }
        }
    
        free(bulk_buf);
    }

    out->ret           = (int32_t) ret;
    out->num_locations = (int32_t) num_chunks;
    out->locations     = bulk_resp_handle;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* find extents rpc handler */
static void find_extents_rpc(hg_handle_t handle)
{
    LOGDBG("find_extents rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_EXTENTS_FIND, handle,
                                  sizeof(find_extents_in_t),
                                  sizeof(find_extents_out_t));
    if (NULL == sreq) {
        find_extents_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_find_extents_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(find_extents_rpc)


/*************************************************************************
 * File attributes request
 *************************************************************************/

/* Get file attributes for target file */
int unifyfs_invoke_metaget_rpc(int gfid,
                               unifyfs_file_attr_t* attrs)
{
    if (NULL == attrs) {
        return EINVAL;
    }

    int owner_rank = hash_gfid_to_server(gfid);
    int need_local_metadata = 0;

    /* do local inode metadata lookup */
    int rc = sm_get_fileattr(gfid, attrs);
    if (owner_rank == glb_pmi_rank) {
         /* local server is the owner */
        return rc;
    } else if (rc == UNIFYFS_SUCCESS) {
        if (attrs->is_laminated) {
            /* if laminated, we already have final metadata locally */
            return UNIFYFS_SUCCESS;
        }

        /* use cached attributes if within threshold */
        struct timespec tp = {0};
        clock_gettime(CLOCK_REALTIME, &tp);
        time_t expire = attrs->last_update + UNIFYFS_METADATA_CACHE_SECONDS;
        if (tp.tv_sec <= expire) {
            LOGINFO("using cached attributes for gfid=%d", gfid);
            return UNIFYFS_SUCCESS;
        } else {
            LOGINFO("cached attributes for gfid=%d have expired "
                    "(now=%d, expiration=%d)", gfid, tp.tv_sec, expire);
        }
    } else if (rc == ENOENT) {
        /* local metaget gave ENOENT, need to create inode if file exists */
        need_local_metadata = 1;
    }

    int ret = UNIFYFS_SUCCESS;
    rc = add_pending_metaget(gfid);
    if (EEXIST == rc) {
        /* wait for pending to finish */
        do {
            LOGDBG("waiting for pending metaget gfid=%d", gfid);
            usleep(10000); /* sleep 10 ms */
        } while (check_pending_metaget(gfid));

        /* should have local copy now if file existed */
        rc = sm_get_fileattr(gfid, attrs);
        return rc;
    } else {
        LOGDBG("added pending metaget gfid=%d", gfid);

        /* forward request to file owner */
        metaget_in_t in;
        metaget_out_t out;
        p2p_request preq;
        hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.metaget_id;
        rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
            goto clear_pending_metaget;
        }

        /* fill rpc input struct and forward request */
        in.gfid = (int32_t) gfid;
        rc = forward_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
            goto clear_pending_metaget;
        }

        /* wait for request completion */
        rc = wait_for_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
            goto clear_pending_metaget;
        }

        /* get the result of the rpc */
        ret = out.ret;
        if (ret == UNIFYFS_SUCCESS) {
            *attrs = out.attr;
            if (out.attr.filename != NULL) {
                attrs->filename = strdup(out.attr.filename);
            }
            if (need_local_metadata) {
                sm_set_fileattr(gfid, UNIFYFS_FILE_ATTR_OP_CREATE, attrs);
            } else {
                sm_set_fileattr(gfid, UNIFYFS_FILE_ATTR_OP_UTIME, attrs);
            }
        }

clear_pending_metaget:
        LOGDBG("clearing pending metaget gfid=%d", gfid);
        rc = clear_pending_metaget(gfid);
        if (rc != UNIFYFS_SUCCESS) {
            LOGWARN("failed to clear pending metaget for gfid=%d", gfid);
        }

        cleanup_p2p_request(&preq);
    }

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
 * File size request
 *************************************************************************/

/*  Get current global size for the target file */
int unifyfs_invoke_filesize_rpc(int gfid,
                                size_t* filesize)
{
    if (NULL == filesize) {
        return EINVAL;
    }
    *filesize = 0;

    int owner_rank = hash_gfid_to_server(gfid);
    int need_local_metadata = 0;

    unifyfs_file_attr_t attrs;
    memset(&attrs, 0, sizeof(attrs));

    /* do local inode metadata lookup to check for laminated */
    int rc = sm_get_fileattr(gfid, &attrs);
    if (owner_rank == glb_pmi_rank) {
        *filesize = (size_t) attrs.size;
        return rc;
    } else if (rc == UNIFYFS_SUCCESS) {
        if (attrs.is_laminated) {
            /* if laminated, we already have final metadata stored locally */
            *filesize = (size_t) attrs.size;
            return UNIFYFS_SUCCESS;
        }

        /* NOTE: unlike metaget above, we don't use cached metadata
         *       for explicit file size lookups */
    } else if (rc == ENOENT) {
        /* local metaget gave ENOENT, need to create inode if file exists */
        need_local_metadata = 1;
    }

    int ret = UNIFYFS_SUCCESS;
    rc = add_pending_metaget(gfid);
    if (EEXIST == rc) {
        /* wait for pending to finish */
        do {
            usleep(10000); /* sleep 10 ms */
        } while (check_pending_metaget(gfid));

        /* should have local copy now if file existed */
        rc = sm_get_fileattr(gfid, &attrs);
        *filesize = (size_t) attrs.size;
        return rc;
    } else {
        /* forward request to file owner */
        metaget_in_t in;
        metaget_out_t out;
        p2p_request preq;
        hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.metaget_id;
        rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
            goto clear_pending_fileattr;
        }

        /* fill rpc input struct and forward request */
        in.gfid = (int32_t) gfid;
        rc = forward_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
            goto clear_pending_fileattr;
        }

        /* wait for request completion */
        rc = wait_for_p2p_request(&preq);
        if (rc != UNIFYFS_SUCCESS) {
            ret = rc;
            goto clear_pending_fileattr;
        }

        /* get the output of the rpc */
        ret = (int) out.ret;
        if (ret == UNIFYFS_SUCCESS) {
            attrs = out.attr;
            *filesize = (size_t) attrs.size;
            if (need_local_metadata) {
                sm_set_fileattr(gfid, UNIFYFS_FILE_ATTR_OP_CREATE, &attrs);
            } else {
                sm_set_fileattr(gfid, UNIFYFS_FILE_ATTR_OP_UTIME, &attrs);
            }
        }

clear_pending_fileattr:
        rc = clear_pending_metaget(gfid);
        if (rc != UNIFYFS_SUCCESS) {
            LOGWARN("failed to clear pending metaget for gfid=%d", gfid);
        }

        cleanup_p2p_request(&preq);
    }

    return ret;
}

static void process_filesize_rpc(server_rpc_req_t* sreq)
{
    const char* rpc_name = "filesize";
    filesize_in_t* in = sreq->req_state->inputs;
    filesize_out_t* out = sreq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* get size of target file */
    size_t filesize = 0;
    int gfid = (int) in->gfid;
    int ret = unifyfs_inode_get_filesize(gfid, &filesize);

    out->ret = (int32_t) ret;
    out->filesize = (hg_size_t) filesize;

    /* send rpc response and cleanup request state */
    sync_respond_server(sreq, rpc_name);
}

/* Filesize rpc handler */
static void filesize_rpc(hg_handle_t handle)
{
    LOGDBG("filesize rpc handler");

    /* create client rpc state */
    server_rpc_req_t* sreq =
        allocate_server_rpc_state(UNIFYFS_SERVER_RPC_FILESIZE, handle,
                                  sizeof(filesize_in_t),
                                  sizeof(filesize_out_t));
    if (NULL == sreq) {
        filesize_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_filesize_rpc(sreq);
}
DEFINE_MARGO_RPC_HANDLER(filesize_rpc)


/*************************************************************************
 * File attributes update request
 *************************************************************************/

/* Set metadata for target file */
int unifyfs_invoke_metaset_rpc(int gfid,
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
    metaset_in_t in;
    metaset_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.metaset_id;
    int rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* fill rpc input struct and forward request */
    in.gfid   = (int32_t) gfid;
    in.fileop = (int32_t) attr_op;
    in.attr   = *attrs;
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
    ret = (int) out.ret;
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
int unifyfs_invoke_laminate_rpc(int gfid)
{
    int ret;
    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        /* I'm the owner, do local inode metadata update */
        return sm_laminate(gfid);
    }

    /* forward request to file owner */
    laminate_in_t in;
    laminate_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.laminate_id;
    int rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* fill rpc input struct and forward request */
    in.gfid = (int32_t) gfid;
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
    ret = (int) out.ret;
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
    transfer_in_t in;
    transfer_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.transfer_id;
    int rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* fill rpc input struct and forward request */
    in.src_rank    = (int32_t) glb_pmi_rank;
    in.client_app  = (int32_t) client_app;
    in.client_id   = (int32_t) client_id;
    in.transfer_id = (int32_t) transfer_id;
    in.gfid        = (int32_t) gfid;
    in.mode        = (int32_t) transfer_mode;
    in.dst_file    = (hg_const_string_t) dest_file;
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
    int ret = (int) out.ret;
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
int unifyfs_invoke_truncate_rpc(int gfid,
                                size_t filesize)
{
    int owner_rank = hash_gfid_to_server(gfid);
    if (owner_rank == glb_pmi_rank) {
        return sm_truncate(gfid, filesize);
    }

    /* forward request to file owner */
    truncate_in_t in;
    truncate_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.truncate_id;
    int rc = init_p2p_request(req_hgid, owner_rank, 
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* fill rpc input struct and forward request */
    in.gfid     = (int32_t) gfid;
    in.filesize = (hg_size_t) filesize;
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
    int ret = (int) out.ret;
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
    server_pid_in_t in;
    server_pid_out_t out;
    p2p_request preq;
    hg_id_t req_hgid = unifyfsd_rpc_context->rpcs.server_pid_id;
    int rc = init_p2p_request(req_hgid, 0,
                              (void*)&in, (void*)&out,
                              &preq);
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

    /* fill rpc input struct and forward request */
    in.rank = glb_pmi_rank;
    in.pid = server_pid;
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
    int ret = (int) out.ret;
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
