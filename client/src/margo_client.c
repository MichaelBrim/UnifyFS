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

/**************************************************************************
 * margo_client.c - Implements the client-server RPC calls (shared-memory)
 **************************************************************************/

#include "unifyfs-internal.h"
#include "unifyfs_rpc_util.h"
#include "margo_client.h"
#include "client_read.h"
#include "client_transfer.h"

/* global rpc context */
static client_rpc_context_t* client_rpc_context; // = NULL

/* register client RPCs */
static void register_client_rpcs(client_rpc_context_t* ctx)
{
    /* shorter name for our margo instance id */
    margo_instance_id mid = ctx->mid;

    hg_id_t hgid;

    /* client-to-server RPCs */

#define CLIENT_REGISTER_RPC(name) \
    do { \
        hgid = MARGO_REGISTER(mid, "unifyfs_" #name "_rpc", \
                              unifyfs_##name##_in_t, \
                              unifyfs_##name##_out_t, \
                              NULL); \
        ctx->rpcs.name##_id = hgid; \
    } while (0)

    CLIENT_REGISTER_RPC(attach);
    CLIENT_REGISTER_RPC(mount);
    CLIENT_REGISTER_RPC(unmount);
    CLIENT_REGISTER_RPC(metaset);
    CLIENT_REGISTER_RPC(metaget);
    CLIENT_REGISTER_RPC(filesize);
    CLIENT_REGISTER_RPC(transfer);
    CLIENT_REGISTER_RPC(truncate);
    CLIENT_REGISTER_RPC(unlink);
    CLIENT_REGISTER_RPC(laminate);
    CLIENT_REGISTER_RPC(fsync);
    CLIENT_REGISTER_RPC(mread);
    CLIENT_REGISTER_RPC(node_local_extents_get);
    CLIENT_REGISTER_RPC(get_gfids);

#undef CLIENT_REGISTER_RPC

    /* server-to-client RPCs */

#define CLIENT_REGISTER_RPC_HANDLER(name) \
    do { \
        hgid = MARGO_REGISTER(mid, "unifyfs_" #name "_rpc", \
                              unifyfs_##name##_in_t, \
                              unifyfs_##name##_out_t, \
                              unifyfs_##name##_rpc); \
        ctx->rpcs.name##_id = hgid; \
    } while (0)

    CLIENT_REGISTER_RPC_HANDLER(heartbeat);
    CLIENT_REGISTER_RPC_HANDLER(mread_req_data);
    CLIENT_REGISTER_RPC_HANDLER(mread_req_complete);
    CLIENT_REGISTER_RPC_HANDLER(transfer_complete);
    CLIENT_REGISTER_RPC_HANDLER(unlink_callback);

#undef CLIENT_REGISTER_RPC_HANDLER
}

/* initialize margo client-server rpc */
int unifyfs_client_rpc_init(int timeout_msecs,
                            int retry_count)
{
    hg_return_t hret;

    if (NULL != client_rpc_context) {
        /* already initialized */
        return UNIFYFS_SUCCESS;
    }

    /* lookup margo server address string,
     * should be something like: "na+sm://7170/0" */
    char* svr_addr_string = rpc_lookup_local_server_addr();
    if (svr_addr_string == NULL) {
        LOGERR("Failed to find local margo RPC server address");
        return UNIFYFS_FAILURE;
    }

    /* duplicate server address string,
     * then parse address to pick out protocol portion
     * which is the piece before the colon like: "na+sm" */
    char* proto = strdup(svr_addr_string);
    char* colon = strchr(proto, ':');
    if (NULL != colon) {
        *colon = '\0';
    }
    LOGDBG("svr_addr:'%s' proto:'%s'", svr_addr_string, proto);

    /* allocate memory for rpc context struct */
    client_rpc_context_t* ctx = calloc(1, sizeof(client_rpc_context_t));
    if (NULL == ctx) {
        LOGERR("Failed to allocate client RPC context");
        free(proto);
        free(svr_addr_string);
        return UNIFYFS_FAILURE;
    }

    /* timeout value to use on rpc operations */
    ctx->timeout_msec = timeout_msecs;
    ctx->retry_count = retry_count;

    /* initialize margo */
    int use_progress_thread = 1;
    int ult_pool_sz = 1;
    ctx->mid = margo_init(proto, MARGO_SERVER_MODE, use_progress_thread,
                          ult_pool_sz);
    assert(ctx->mid);

    /* get server margo address */
    ctx->svr_addr = HG_ADDR_NULL;
    margo_addr_lookup(ctx->mid, svr_addr_string, &(ctx->svr_addr));

    /* done with the protocol and address strings, free them */
    free(proto);
    free(svr_addr_string);

    /* check that we got a valid margo address for the server */
    if (ctx->svr_addr == HG_ADDR_NULL) {
        LOGERR("Failed to resolve margo server RPC address");
        margo_finalize(ctx->mid);
        free(ctx);
        return UNIFYFS_FAILURE;
    }

    /* get our own margo address */
    hret = margo_addr_self(ctx->mid, &(ctx->client_addr));
    if (hret != HG_SUCCESS) {
        LOGERR("Failed to acquire our margo address");
        margo_addr_free(ctx->mid, ctx->svr_addr);
        margo_finalize(ctx->mid);
        free(ctx);
        return UNIFYFS_FAILURE;
    }

    /* convert our margo address to a string */
    char addr_self_string[128];
    hg_size_t addr_self_string_sz = sizeof(addr_self_string);
    hret = margo_addr_to_string(ctx->mid,
        addr_self_string, &addr_self_string_sz, ctx->client_addr);
    if (hret != HG_SUCCESS) {
        LOGERR("Failed to convert our margo address to string");
        margo_addr_free(ctx->mid, ctx->client_addr);
        margo_addr_free(ctx->mid, ctx->svr_addr);
        margo_finalize(ctx->mid);
        free(ctx);
        return UNIFYFS_FAILURE;
    }

    /* make a copy of our own margo address string */
    ctx->client_addr_str = strdup(addr_self_string);

    /* look up and record id values for each rpc */
    register_client_rpcs(ctx);

    /* cache context in global variable */
    client_rpc_context = ctx;

    return UNIFYFS_SUCCESS;
}

/* free resources allocated in corresponding call
 * to unifyfs_client_rpc_init, frees structure
 * allocated and sets pcontect to NULL */
int unifyfs_client_rpc_finalize(void)
{
    if (client_rpc_context != NULL) {
        /* define a temporary to refer to context */
        client_rpc_context_t* ctx = client_rpc_context;
        client_rpc_context = NULL;

        /* free margo address for client */
        margo_addr_free(ctx->mid, ctx->client_addr);

        /* free margo address to server */
        margo_addr_free(ctx->mid, ctx->svr_addr);

        /* shut down margo */
        margo_finalize(ctx->mid);

        /* free memory allocated for context structure */
        free(ctx->client_addr_str);
        free(ctx);
    }

    return UNIFYFS_SUCCESS;
}

/*--- Invocation methods for client-to-server RPCs ---*/

#if 0 /* create and return a margo handle for given rpc id */
static hg_handle_t create_handle(hg_id_t id)
{
    /* define a temporary to refer to global context */
    client_rpc_context_t* ctx = client_rpc_context;

    /* create handle for specified rpc */
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_return_t hret = margo_create(ctx->mid, ctx->svr_addr, id, &handle);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_create() failed");
    }
    return handle;
}
#endif

static int sync_call_server(rpc_state* rpc, const char* rpc_name)
{
    int ret = UNIFYFS_SUCCESS;
    LOGDBG("calling the %s rpc(%p) synchronously", rpc_name, rpc);
    int rc = sync_rpc_request(rpc, client_rpc_context->timeout_msec,
                              client_rpc_context->retry_count);
    if (rc != 0) {
        LOGERR("%s sync rpc(%p) request failed (rc=%d)", rpc_name, rpc, rc);
        ret = rc;
    }
    return ret;
}

#if 0 // TODO: determine if we need async client-server rpcs
static int async_call_server(rpc_state* rpc, const char* rpc_name)
{
    int ret = UNIFYFS_SUCCESS;
    LOGDBG("calling the %s rpc(%p) asynchronously", rpc_name, rpc);
    int rc = async_rpc_request(rpc, client_rpc_context->timeout_msec);
    if (rc != 0) {
        LOGERR("%s async rpc(%p) request failed (rc=%d)", rpc_name, rpc, rc);
        ret = rc;
    }
    return ret;
}

static int async_call_finish(rpc_state* rpc, const char* rpc_name)
{
    int ret = UNIFYFS_SUCCESS;
    LOGDBG("finishing the async %s rpc(%p)", rpc_name, rpc);
    int rc = async_rpc_request_finish(rpc);
    if (rc != 0) {
        LOGERR("%s async rpc(%p) request finish failed (rc=%d)",
               rpc_name, rpc, rc);
        ret = rc;
    }
    return ret;
}
#endif

/* invokes the mount rpc function */
int invoke_client_mount_rpc(unifyfs_client* client)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_mount";
    unifyfs_mount_in_t in;
    unifyfs_mount_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.mount_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.dbg_rank = client->state.app_rank;
    in.mount_prefix = strdup(client->cfg.unifyfs_mountpoint);
    in.client_addr_str = strdup(client_rpc_context->client_addr_str);
    
    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
        if (ret == (int)UNIFYFS_SUCCESS) {
            /* get assigned client id, and verify app_id */
            client->state.client_id = (int) out.client_id;
            int srvr_app_id = (int) out.app_id;
            if (client->state.app_id != srvr_app_id) {
                LOGWARN("mismatch on app_id - using %d, server returned %d",
                        client->state.app_id, srvr_app_id);
            }
            LOGINFO("My [app:client] id is [%d:%d]",
                    client->state.app_id, client->state.client_id);
        }
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    /* free allocated memory from input struct */
    free((void*)in.mount_prefix);
    free((void*)in.client_addr_str);

    return ret;
}


/* invokes the attach rpc function */
int invoke_client_attach_rpc(unifyfs_client* client)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_attach";
    unifyfs_attach_in_t in;
    unifyfs_attach_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.attach_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id            = client->state.app_id;
    in.client_id         = client->state.client_id;
    in.shmem_super_size  = client->state.shm_super_ctx->size;
    in.meta_offset       = client->state.write_index.index_offset;
    in.meta_size         = client->state.write_index.index_size;

    if (NULL != client->state.logio_ctx->shmem) {
        in.logio_mem_size = client->state.logio_ctx->shmem->size;
    } else {
        in.logio_mem_size = 0;
    }

    in.logio_spill_size = client->state.logio_ctx->spill_sz;
    if (client->state.logio_ctx->spill_sz) {
        in.logio_spill_dir = strdup(client->cfg.logio_spill_dir);
    } else {
        in.logio_spill_dir = NULL;
    }

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    /* free allocated memory from input struct */
    if (NULL != in.logio_spill_dir) {
        free((void*)in.logio_spill_dir);
    }

    return ret;
}

/* function invokes the unmount rpc */
int invoke_client_unmount_rpc(unifyfs_client* client)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_attach";
    unifyfs_unmount_in_t in;
    unifyfs_unmount_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.unmount_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/*
 * Set the metadata values for a file (after optionally creating it).
 * The gfid for the file is in f_meta->gfid.
 *
 * create: If set to 1, attempt to create the file first.  If the file
 *         already exists, then update its metadata with the values in
 *         f_meta.  If set to 0, and the file does not exist, then
 *         the server will return an error.
 *
 * f_meta: The metadata values to update.
 */
int invoke_client_metaset_rpc(unifyfs_client* client,
                              unifyfs_file_attr_op_e attr_op,
                              unifyfs_file_attr_t* f_meta)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_metaset";
    unifyfs_metaset_in_t in;
    unifyfs_metaset_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.metaset_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.attr_op   = (int32_t) attr_op;
    memcpy(&(in.attr), f_meta, sizeof(*f_meta));

    LOGDBG("metaset - gfid:%d file:%s", in.attr.gfid, in.attr.filename);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client metaget rpc function */
int invoke_client_metaget_rpc(unifyfs_client* client,
                              int gfid,
                              unifyfs_file_attr_t* file_meta)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_metaget";
    unifyfs_metaget_in_t in;
    unifyfs_metaget_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.metaget_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.gfid      = (int32_t) gfid;

    LOGDBG("metaget - gfid:%d", gfid);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
        if (ret == (int)UNIFYFS_SUCCESS) {
            /* fill in results  */
            memset(file_meta, 0, sizeof(unifyfs_file_attr_t));
            *file_meta = out.attr;
            if (NULL != out.attr.filename) {
                file_meta->filename = strdup(out.attr.filename);
            }
        }
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client filesize rpc function */
int invoke_client_filesize_rpc(unifyfs_client* client,
                               int gfid,
                               size_t* outsize)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_filesize";
    unifyfs_filesize_in_t in;
    unifyfs_filesize_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.filesize_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.gfid      = (int32_t) gfid;

    LOGDBG("getting filesize - gfid:%d", gfid);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
        if (ret == (int)UNIFYFS_SUCCESS) {
            *outsize = (size_t) out.filesize;
        }
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc); 

    return ret;
}

/* invokes the client transfer rpc function */
int invoke_client_transfer_rpc(unifyfs_client* client,
                               int transfer_id,
                               int gfid,
                               int parallel_transfer,
                               const char* dest_file)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_transfer";
    unifyfs_transfer_in_t in;
    unifyfs_transfer_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.transfer_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id      = (int32_t) client->state.app_id;
    in.client_id   = (int32_t) client->state.client_id;
    in.transfer_id = (int32_t) transfer_id;
    in.gfid        = (int32_t) gfid;
    in.mode        = (int32_t) parallel_transfer;
    in.dst_file    = (hg_const_string_t) dest_file;

    LOGDBG("transferring gfid:%d to %s", gfid, dest_file);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client truncate rpc function */
int invoke_client_truncate_rpc(unifyfs_client* client,
                               int gfid,
                               size_t filesize)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_truncate";
    unifyfs_truncate_in_t in;
    unifyfs_truncate_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.truncate_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.gfid      = (int32_t) gfid;
    in.filesize  = (hg_size_t) filesize;

    LOGDBG("truncate - gfid:%d", gfid);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client unlink rpc function */
int invoke_client_unlink_rpc(unifyfs_client* client,
                             int gfid)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_unlink";
    unifyfs_unlink_in_t in;
    unifyfs_unlink_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.unlink_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.gfid      = (int32_t) gfid;

    LOGDBG("unlink - gfid:%d", gfid);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client-to-server laminate rpc function */
int invoke_client_laminate_rpc(unifyfs_client* client,
                               int gfid)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_laminate";
    unifyfs_laminate_in_t in;
    unifyfs_laminate_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.laminate_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.gfid      = (int32_t) gfid;

    LOGDBG("laminate - gfid:%d", gfid);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client sync rpc function */
int invoke_client_sync_rpc(unifyfs_client* client,
                           int gfid)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_fsync";
    unifyfs_fsync_in_t in;
    unifyfs_fsync_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.fsync_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.gfid      = (int32_t) gfid;

    LOGDBG("sync - gfid:%d", gfid);

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client mread rpc function */
int invoke_client_mread_rpc(unifyfs_client* client,
                            unsigned int reqid,
                            int read_count,
                            size_t extents_size,
                            void* extents_buffer)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_mread";
    unifyfs_mread_in_t in;
    unifyfs_mread_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.mread_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* initialize bulk handle for extents */
    hg_return_t hret = margo_bulk_create(rpc->mid,
                                         1, &extents_buffer, &extents_size,
                                         HG_BULK_READ_ONLY, &in.bulk_extents);
    if (hret != HG_SUCCESS) {
        LOGERR("failed to create bulk for %s rpc request - %s",
               rpc_name, HG_Error_to_string(hret));
        cleanup_rpc_state(rpc);
        return UNIFYFS_ERROR_MARGO;
    }

    /* set input parameters */
    in.mread_id   = (int32_t) reqid;
    in.app_id     = (int32_t) client->state.app_id;
    in.client_id  = (int32_t) client->state.client_id;
    in.read_count = (int32_t) read_count;
    in.bulk_size  = (hg_size_t) extents_size;

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
    } else {
        ret = rc;
    }

    margo_bulk_free(in.bulk_extents);
    cleanup_rpc_state(rpc);

    return ret;
}

/* invokes the client metaget rpc function */
int invoke_client_node_local_extents_get_rpc(unifyfs_client* client,
                                             int num_req,
                                             extents_list_t* read_req,
                                             size_t* extent_count,
                                             unifyfs_client_index_t** extents)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    size_t extents_size = num_req * sizeof(unifyfs_extent_t);
    void* buffer = malloc(extents_size);
    if (NULL == buffer) {
        return ENOMEM;
    }

    unifyfs_extent_t* int_extents = (unifyfs_extent_t*)buffer;
    extents_list_t* cur = read_req;
    for (int i = 0; i < num_req; i++) {
        unifyfs_extent_t* ext = int_extents + i;
        ext->gfid = cur->value.gfid;
        ext->offset = cur->value.file_pos;
        ext->length = cur->value.length;
        cur = cur->next;
    }

    const char* rpc_name = "unifyfs_node_local_extents_get";
    unifyfs_node_local_extents_get_in_t in;
    unifyfs_node_local_extents_get_out_t out;
    rpc_state* rpc =
        create_rpc_request(client_rpc_context->rpcs.node_local_extents_get_id,
                           client_rpc_context->mid,
                           client_rpc_context->svr_addr,
                           (void*)&in, 0,
                           (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        free(buffer);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    hg_return_t hret = margo_bulk_create(rpc->mid,
                                         1, &buffer, &extents_size,
                                         HG_BULK_READ_ONLY, &in.bulk_data);
    if (hret != HG_SUCCESS) {
        LOGERR("failed to create bulk for %s rpc request - %s",
               rpc_name, HG_Error_to_string(hret));
        cleanup_rpc_state(rpc);
        free(buffer);
        return UNIFYFS_ERROR_MARGO;
    }
    in.app_id = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;
    in.num_req = num_req;
    in.bulk_size = extents_size;

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
        if (ret == (int) UNIFYFS_SUCCESS) {
            *extent_count = out.extent_count;
            void* out_buffer = pull_margo_bulk(rpc->handle, out.bulk_data,
                                               out.bulk_size, NULL);
            *extents = (unifyfs_client_index_t*) out_buffer;
        }
    } else {
        ret = rc;
    }

    margo_bulk_free(in.bulk_data);
    cleanup_rpc_state(rpc);
    free(buffer);
    return ret;
}

/* invokes the get_gfids rpc function */
int invoke_client_get_gfids_rpc(unifyfs_client* client,
                                int* num_gfids,
                                int** gfid_list)
{
    /* check that we have initialized margo */
    if (NULL == client_rpc_context) {
        return UNIFYFS_FAILURE;
    }

    const char* rpc_name = "unifyfs_get_gfids";
    unifyfs_get_gfids_in_t in;
    unifyfs_get_gfids_out_t out;
    rpc_state* rpc = create_rpc_request(client_rpc_context->rpcs.get_gfids_id,
                                        client_rpc_context->mid,
                                        client_rpc_context->svr_addr,
                                        (void*)&in, 0,
                                        (void*)&out, 0);
    if (NULL == rpc) {
        LOGERR("failed to create %s rpc request", rpc_name);
        return UNIFYFS_FAILURE;
    }

    /* set input parameters */
    in.app_id    = (int32_t) client->state.app_id;
    in.client_id = (int32_t) client->state.client_id;

    /* call rpc function */
    int ret;
    int rc = sync_call_server(rpc, rpc_name);
    if (rc == UNIFYFS_SUCCESS) {
        LOGDBG("%s got response ret=%" PRIi32, rpc_name, out.ret);
        ret = (int) out.ret;
        if (ret == (int) UNIFYFS_SUCCESS) {
            LOGDBG("Number of GFIDs returned: %d", out.num_gfids);
            *num_gfids = (int) out.num_gfids;
            hg_size_t buf_size = (size_t) out.num_gfids * sizeof(int);
            void* out_buffer = pull_margo_bulk(rpc->handle, out.bulk_gfids,
                                               buf_size, NULL);
            *gfid_list = (int*) out_buffer;
        } else {
            *num_gfids = 0;
            *gfid_list = NULL;
        }
    } else {
        ret = rc;
    }

    cleanup_rpc_state(rpc);
    return ret;
}


/*--- Handler methods for server-to-client RPCs ---*/

/* simple heartbeat ping rpc */
static void unifyfs_heartbeat_rpc(hg_handle_t handle)
{
    int ret;

    /* get input params */
    unifyfs_heartbeat_in_t in;
    hg_return_t hret = margo_get_input(handle, &in);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_get_input() failed");
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        /* lookup client */
        unifyfs_client* client;
        int client_app = (int) in.app_id;
        int client_id  = (int) in.client_id;
        client = unifyfs_find_client(client_app, client_id, NULL);
        if (NULL == client) {
            /* unknown client */
            ret = EINVAL;
        } else if (client->state.is_mounted) {
            /* client is still active */
            ret = UNIFYFS_SUCCESS;
        } else {
            ret = UNIFYFS_FAILURE;
        }
        margo_free_input(handle, &in);
    }

    /* set rpc result status */
    unifyfs_heartbeat_out_t out;
    out.ret = ret;

    /* return to caller */
    LOGDBG("responding");
    hret = margo_respond(handle, &out);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_respond() failed");
    }

    /* free margo resources */
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_heartbeat_rpc)

/* for client read request identified by mread_id and request index, copy bulk
 * data to request's user buffer at given byte offset from start of request */
static void unifyfs_mread_req_data_rpc(hg_handle_t handle)
{
    int ret = UNIFYFS_SUCCESS;

    /* get input params */
    unifyfs_mread_req_data_in_t in;
    hg_return_t hret = margo_get_input(handle, &in);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_get_input() failed");
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        /* lookup client mread request */
        unifyfs_client* client;
        int client_app   = (int) in.app_id;
        int client_id    = (int) in.client_id;
        int client_mread = (int) in.mread_id;
        client = unifyfs_find_client(client_app, client_id, NULL);
        client_mread_status* mread = client_get_mread_status(client,
                                                             client_mread);
        if (NULL == mread) {
            /* unknown client request */
            ret = EINVAL;
        } else {
            int read_index = (int) in.read_index;
            size_t data_size = (size_t) in.bulk_size;
            size_t data_offset = (size_t) in.read_offset;

            if (data_size != 0) {
                /* set up pointer to user buffer at read req offset */
                ABT_mutex_lock(mread->sync);
                assert(read_index < mread->n_reads);
                read_req_t* rdreq = mread->reqs + read_index;
                void* user_buf = (void*)(rdreq->buf + data_offset);
                size_t data_space = rdreq->length - data_offset;
                ABT_mutex_unlock(mread->sync);

                if (data_size > data_space) {
                    LOGERR("data size exceeds available user buffer space");
                    ret = EINVAL;
                } else {
                    /* get margo/mercury info to set up bulk transfer */
                    const struct hg_info* hgi = margo_get_info(handle);
                    assert(hgi);
                    margo_instance_id mid =
                        margo_hg_handle_get_instance(handle);
                    assert(mid != MARGO_INSTANCE_NULL);

                    /* register user buffer for bulk access */
                    hg_bulk_t bulk_local;
                    hret = margo_bulk_create(mid, 1, &user_buf, &data_size,
                                             HG_BULK_WRITE_ONLY, &bulk_local);
                    if (hret != HG_SUCCESS) {
                        LOGERR("margo_bulk_create() failed");
                        ret = UNIFYFS_ERROR_MARGO;
                    } else {
                        /* execute the transfer to pull data from remote side
                         * into our local buffer.
                         *
                         * NOTE: mercury/margo bulk transfer does not check the
                         * maximum transfer size that the underlying transport
                         * supports, and a large bulk transfer may result in
                         * failure. */
                        int i = 0;
                        hg_size_t offset, len;
                        hg_size_t remain = in.bulk_size;
                        hg_size_t max_bulk = UNIFYFS_SERVER_MAX_BULK_TX_SIZE;
                        do {
                            offset = i * max_bulk;
                            len = (remain < max_bulk) ? remain : max_bulk;
                            hret = margo_bulk_transfer(mid, HG_BULK_PULL,
                                                       hgi->addr,
                                                       in.bulk_data, offset,
                                                       bulk_local, offset,
                                                       len);
                            if (hret != HG_SUCCESS) {
                                LOGERR("margo_bulk_transfer(buf_offset=%zu, "
                                       "len=%zu) failed",
                                       (size_t)offset, (size_t)len);
                                ret = UNIFYFS_ERROR_MARGO;
                                break;
                            }
                            remain -= len;
                            i++;
                        } while (remain > 0);

                        if (hret == HG_SUCCESS) {
                            ABT_mutex_lock(mread->sync);
                            update_read_req_coverage(rdreq, data_offset,
                                                     data_size);
                            ABT_mutex_unlock(mread->sync);
                            LOGINFO("updated coverage for mread[%d] request %d",
                                   client_mread, read_index);
                        }
                        margo_bulk_free(bulk_local);
                    }
                }
            }
        }
        margo_free_input(handle, &in);
    }

    /* set rpc result status */
    unifyfs_mread_req_data_out_t out;
    out.ret = ret;

    /* return to caller */
    LOGDBG("responding");
    hret = margo_respond(handle, &out);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_respond() failed");
    }

    /* free margo resources */
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_mread_req_data_rpc)

/* for client read request identified by mread_id and request index,
 * update request completion state according to input params */
static void unifyfs_mread_req_complete_rpc(hg_handle_t handle)
{
    int ret = UNIFYFS_SUCCESS;

    /* get input params */
    unifyfs_mread_req_complete_in_t in;
    hg_return_t hret = margo_get_input(handle, &in);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_get_input() failed");
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        /* lookup client mread request */
        unifyfs_client* client;
        int client_app   = (int) in.app_id;
        int client_id    = (int) in.client_id;
        int client_mread = (int) in.mread_id;
        client = unifyfs_find_client(client_app, client_id, NULL);
        client_mread_status* mread = client_get_mread_status(client,
                                                             client_mread);
        if (NULL == mread) {
            /* unknown client request */
            ret = EINVAL;
        } else {
            int read_index = (int) in.read_index;
            int read_error = (int) in.read_error;
            int complete = 1;

            /* Update the mread state, which will signal completion if all data
             * has been processed for all the requests in the mread */
            ret = client_update_mread_request(mread, read_index,
                                              complete, read_error);
        }
        margo_free_input(handle, &in);
    }

    /* set rpc result status */
    unifyfs_mread_req_complete_out_t out;
    out.ret = ret;

    LOGDBG("responding");

    /* return to caller */
    hret = margo_respond(handle, &out);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_respond() failed");
    }

    /* free margo resources */
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_mread_req_complete_rpc)

/* for client transfer request identified by transfer_id,
 * update request completion state according to input params */
static void unifyfs_transfer_complete_rpc(hg_handle_t handle)
{
    int ret = UNIFYFS_SUCCESS;

    /* get input params */
    unifyfs_transfer_complete_in_t in;
    hg_return_t hret = margo_get_input(handle, &in);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_get_input() failed");
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        /* lookup client transfer request */
        unifyfs_client* client;
        int client_app     = (int) in.app_id;
        int client_id      = (int) in.client_id;
        int transfer_id    = (int) in.transfer_id;
        size_t transfer_sz = (size_t) in.transfer_size_bytes;
        uint32_t xfer_sec  = (uint32_t) in.transfer_time_sec;
        uint32_t xfer_usec = (uint32_t) in.transfer_time_usec;
        int error_code     = (int) in.error_code;
        client = unifyfs_find_client(client_app, client_id, NULL);
        if (NULL == client) {
            /* unknown client */
            ret = EINVAL;
        } else {
            /* Update the transfer state */
            double transfer_time = (double) xfer_sec;
            transfer_time += (double) xfer_usec / 1000000.0;
            ret = client_complete_transfer(client, transfer_id, error_code,
                                           transfer_sz, transfer_time);
        }
        margo_free_input(handle, &in);
    }

    /* set rpc result status */
    unifyfs_transfer_complete_out_t out;
    out.ret = ret;

    LOGDBG("responding");

    /* return to caller */
    hret = margo_respond(handle, &out);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_respond() failed");
    }

    /* free margo resources */
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_transfer_complete_rpc)

/* unlink callback rpc */
static void unifyfs_unlink_callback_rpc(hg_handle_t handle)
{
    int ret;

    /* get input params */
    unifyfs_unlink_callback_in_t in;
    hg_return_t hret = margo_get_input(handle, &in);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_get_input() failed");
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        /* lookup client */
        unifyfs_client* client;
        int client_app = (int) in.app_id;
        int client_id  = (int) in.client_id;
        client = unifyfs_find_client(client_app, client_id, NULL);
        if (NULL == client) {
            /* unknown client */
            ret = EINVAL;
        } else {
            int gfid = (int) in.gfid;
            int fid = unifyfs_fid_from_gfid(client, gfid);
            if (-1 != fid) {
                unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(client,
                                                                     fid);
                if ((meta != NULL) && (fid == meta->fid)) {
                    meta->pending_unlink = 1;
                }
            }
            ret = UNIFYFS_SUCCESS;
        }
        margo_free_input(handle, &in);
    }

    /* set rpc result status */
    unifyfs_unlink_callback_out_t out;
    out.ret = ret;

    /* return to caller */
    LOGDBG("responding");
    hret = margo_respond(handle, &out);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_respond() failed");
    }

    /* free margo resources */
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_unlink_callback_rpc)
