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

/*
 * Copyright (c) 2017, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Copyright (c) 2017, Florida State University. Contributions from
 * the Computer Architecture and Systems Research Laboratory (CASTL)
 * at the Department of Computer Science.
 *
 * Written by: Teng Wang, Adam Moody, Weikuan Yu, Kento Sato, Kathryn Mohror
 * LLNL-CODE-728877. All rights reserved.
 *
 * This file is part of burstfs.
 * For details, see https://github.com/llnl/burstfs
 * Please read https://github.com/llnl/burstfs/LICENSE for full license text.
 */

// system headers
#include <fcntl.h>
#include <libgen.h>
#include <sys/mman.h>

// server components
#include "unifyfs_global.h"
#include "unifyfs_request_manager.h"
#include "unifyfs_group_rpc.h"
#include "unifyfs_p2p_rpc.h"

// margo rpcs
#include "margo_server.h"
#include "unifyfs_client_rpcs.h"
#include "unifyfs_rpc_util.h"
#include "unifyfs_misc.h"


// utility functions
client_rpc_req_t* allocate_client_rpc_state(client_rpc_e rpc_type,
                                            hg_handle_t handle,
                                            size_t input_sz,
                                            size_t output_sz)
{
    client_rpc_req_t* creq = (client_rpc_req_t*)
        calloc(1, sizeof(client_rpc_req_t));
    if (NULL != creq) {
        creq->req_type = rpc_type;
        void* input = calloc(1, input_sz);
        if (NULL != input) {
            if (HG_HANDLE_NULL != handle) {
                hg_return_t hret = margo_get_input(handle, input);
                if (hret != HG_SUCCESS) {
                    LOGERR("margo_get_input() failed - %s",
                           HG_Error_to_string(hret));
                    free(input);
                    free(creq);
                    creq = NULL;
                }
            }
            if (NULL != creq) {
                rpc_state* state = create_rpc_response(handle, input,
                                                       NULL, output_sz);
                if (NULL != state) {
                    creq->req_state = state;
                } else {
                    if (HG_HANDLE_NULL != handle)
                        margo_free_input(handle, input);
                    free(input);
                    free(creq);
                    creq = NULL;
                }
            }
        } else {
            free(creq);
            creq = NULL;
        }
    }
    return creq;
}

void release_client_rpc_state(client_rpc_req_t* creq)
{
    if (NULL != creq) {
        if (NULL != creq->req_state) {
            cleanup_rpc_state(creq->req_state);
            creq->req_state = NULL;
        }
        free(creq);
    }
}

void sync_respond_client(client_rpc_req_t* creq, const char* rpc_name)
{
    rpc_state* rpc = creq->req_state;
    LOGDBG("responding to the %s client-server rpc(%p)",
           rpc_name, rpc);
    int rc = sync_rpc_response(rpc, margo_client_retry_count);
    if (rc != 0) {
        LOGERR("synchronous %s rpc(%p) response failed (rc=%d)",
               rpc_name, rpc, rc);
    }
    release_client_rpc_state(creq);
}

int async_respond_client(client_rpc_req_t* creq, const char* rpc_name)
{
    int ret = UNIFYFS_SUCCESS;
    rpc_state* rpc = creq->req_state;
    LOGDBG("responding to the %s client-server rpc(%p) asynchronously",
           rpc_name, rpc);
    int rc = async_rpc_response(rpc, margo_client_retry_count);
    if (rc != 0) {
        LOGERR("%s async rpc(%p) response failed (rc=%d)",
               rpc_name, rpc, rc);
        ret = rc;
    }
    return ret;
}

void async_respond_client_finish(client_rpc_req_t* creq, const char* rpc_name)
{
    rpc_state* rpc = creq->req_state;
    LOGDBG("finishing the async response %s rpc(%p)", rpc_name, rpc);
    int rc = async_rpc_response_finish(rpc);
    if (rc != 0) {
        LOGERR("%s async rpc(%p) response finish failed (rc=%d)",
               rpc_name, rpc, rc);
    }
    release_client_rpc_state(creq);
}


#if 0 // NOT YET USED
static int sync_call_client(rpc_state* rpc, const char* rpc_name)
{
    int ret = UNIFYFS_SUCCESS;
    LOGDBG("calling the %s server-client rpc(%p)",
           rpc_name, rpc);
    int rc = sync_rpc_request(rpc, margo_client_timeout_msec,
                              margo_client_retry_count);
    if (rc != 0) {
        LOGERR("synchronous %s rpc(%p) request failed (rc=%d)",
               rpc_name, rpc, rc);
        ret = rc;
    }
    return ret;
}
#endif

#if 0 // TODO: determine if we need server-client async request rpcs
static int async_call_client(rpc_state* rpc, const char* rpc_name)
{
    int ret = UNIFYFS_SUCCESS;
    LOGDBG("calling the %s server-client rpc(%p) asynchronously",
           rpc_name, rpc);
    int rc = async_rpc_request(rpc, margo_client_timeout_msec);
    if (rc != 0) {
        LOGERR("%s async rpc(%p) request failed (rc=%d)",
               rpc_name, rpc, rc);
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


void process_client_attach_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_attach";
    unifyfs_attach_in_t* in = creq->req_state->inputs;
    unifyfs_attach_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* lookup client structure and attach it */
    int app_id = (int) in->app_id;
    int client_id = (int) in->client_id;
    app_client* client = get_app_client(app_id, client_id);
    if (NULL != client) {
        LOGDBG("attaching client %d:%d", app_id, client_id);
        ret = attach_app_client(client,
                                in->logio_spill_dir,
                                in->logio_spill_size,
                                in->logio_mem_size,
                                in->shmem_super_size,
                                in->meta_offset,
                                in->meta_size);
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("attach_app_client() failed");
        } else {
            client->reqmgr->attached = 1;
        }
    } else {
        LOGERR("client not found (app_id=%d, client_id=%d)",
               app_id, client_id);
        ret = (int)UNIFYFS_FAILURE;
    }
    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
    
}


static void create_directory(int app_id,
                             int client_id,
                             const char* dirpath)
{
    /* initialize an empty file attributes structure */
    unifyfs_file_attr_t fattr;
    unifyfs_file_attr_set_invalid(&fattr);

    /* set global file id and path */
    int gfid = unifyfs_generate_gfid(dirpath);
    fattr.gfid = gfid;
    fattr.filename = strdup(dirpath);

    /* set initial directory state */
    fattr.mode = UNIFYFS_STAT_DEFAULT_DIR_MODE;
    fattr.is_shared = 1;
    fattr.is_laminated = 0;
    fattr.size = 0;

    /* use current time for atime/mtime/ctime */
    struct timespec tp = {0};
    clock_gettime(CLOCK_REALTIME, &tp);
    fattr.atime = tp;
    fattr.mtime = tp;
    fattr.ctime = tp;
    fattr.last_update = tp.tv_sec;

    /* capture current uid and gid */
    fattr.uid = getuid();
    fattr.gid = getgid();

    unifyfs_fops_ctx_t ctx = {
        .client_req = NULL,
        .app_id     = app_id,
        .client_id  = client_id,
    };
    int attr_op = UNIFYFS_FILE_ATTR_OP_CREATE;
    int rc = unifyfs_fops_metaset(&ctx, app_id, attr_op, &fattr);
    if (rc != UNIFYFS_SUCCESS) {
        if (rc != EEXIST)
            LOGDBG("unifyfs_fops_metaset() failed to create directory %s - %s",
                   dirpath, unifyfs_rc_enum_str(rc));
    } else {
        LOGDBG("created metadata for directory:");
        debug_print_file_attr(&fattr);
    }

    if (NULL != fattr.filename) {
        free(fattr.filename);
    }
}

void process_client_mount_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;
    int app_id = -1;
    int client_id = -1;
    int create_mountpoint = 0;

    const char* rpc_name = "unifyfs_mount";
    unifyfs_mount_in_t* in = creq->req_state->inputs;
    unifyfs_mount_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* read app_id and client_id from input */
    app_id = unifyfs_generate_gfid(in->mount_prefix);

    /* lookup app_config for given app_id */
    app_config* app_cfg = get_application(app_id);
    if (app_cfg == NULL) {
        /* insert new app_config into our app_configs array */
        LOGDBG("creating new application for app_id=%d", app_id);
        app_cfg = new_application(app_id, &create_mountpoint);
        if (NULL == app_cfg) {
            ret = UNIFYFS_FAILURE;
        }
    } else {
        LOGDBG("using existing app_config for app_id=%d", app_id);
    }

    if (NULL != app_cfg) {
        LOGDBG("creating new app client for %s", in->client_addr_str);
        app_client* client = new_app_client(app_cfg,
                                            in->client_addr_str,
                                            in->dbg_rank);
        if (NULL == client) {
            LOGERR("failed to create new client for app_id=%d dbg_rank=%d",
                   app_id, (int)in->dbg_rank);
            ret = (int)UNIFYFS_FAILURE;
        } else {
            client_id = client->state.client_id;
            LOGDBG("created new application client [%d:%d]",
                   app_id, client_id);
            if (create_mountpoint) {
                create_directory(app_id, client_id, in->mount_prefix);
            }
        }
    }

    out->app_id = (int32_t) app_id;
    out->client_id = (int32_t) client_id;
    out->ret = ret;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
    
}

void process_client_unmount_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_unmount";
    unifyfs_unmount_in_t* in = creq->req_state->inputs;
    unifyfs_unmount_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* read app_id and client_id from input */
    int app_id    = in->app_id;
    int client_id = in->client_id;

    /* disconnect app client */
    app_client* clnt = get_app_client(app_id, client_id);
    if (NULL != clnt) {
        LOGDBG("disconnecting app client [%d:%d]", app_id, client_id);
        ret = disconnect_app_client(clnt);
    } else {
        LOGERR("application client not found");
        ret = EINVAL;
    }

    out->ret = ret;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
    
}

void process_client_metaget_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_metaget";
    unifyfs_metaget_in_t* in = creq->req_state->inputs;
    unifyfs_metaget_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));
    
    int gfid = (int) in->gfid;
    LOGDBG("getting metadata for gfid=%d", gfid);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };

    unifyfs_file_attr_t fattr;
    memset(&fattr, 0, sizeof(fattr));

    ret = unifyfs_fops_metaget(&ctx, gfid, &fattr);
    if (ret != UNIFYFS_PENDING) {
        out->ret = (int32_t) ret;
        if (ret == UNIFYFS_SUCCESS) {
            out->attr = fattr;
        } else {
            LOGDBG("unifyfs_fops_metaget() failed");
        }
        /* send rpc response and cleanup request state */
        sync_respond_client(creq, rpc_name);
    }
    // else, some other thread will respond when pending req finishes
}

void process_client_metaset_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_metaset";
    unifyfs_metaset_in_t* in = creq->req_state->inputs;
    unifyfs_metaset_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    int gfid = in->attr.gfid;
    int attr_op = (int) in->attr_op;

    LOGDBG("setting metadata (op=%d) for gfid=%d", attr_op, gfid);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };
    
    unifyfs_file_attr_t fattr = in->attr;
    if (NULL != in->attr.filename) {
        fattr.filename = strdup(in->attr.filename);
        if ((attr_op == UNIFYFS_FILE_ATTR_OP_CREATE) &&
            (fattr.mode == UNIFYFS_STAT_DEFAULT_FILE_MODE)) {
            /* attempt to create parent directory */
            char* filecopy = strdup(fattr.filename);
            char* dirpath = dirname(filecopy);
            if (0 != strcmp(dirpath, ".")) {
                create_directory(ctx.app_id, ctx.client_id, dirpath);
            }
            free(filecopy);
        }
    }

    ret = unifyfs_fops_metaset(&ctx, gfid, attr_op, &fattr);
    if (ret != UNIFYFS_PENDING) {
        if (ret != UNIFYFS_SUCCESS) {
            LOGDBG("unifyfs_fops_metaset() failed");
        }

        if (NULL != fattr.filename) {
            free(fattr.filename);
        }
        
        out->ret = (int32_t) ret;

        /* send rpc response and cleanup request state */
        sync_respond_client(creq, rpc_name);
    }
    // else, some other thread will respond when pending req finishes
}

void process_client_filesize_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_filesize";
    unifyfs_filesize_in_t* in = creq->req_state->inputs;
    unifyfs_filesize_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));
    
    int gfid = (int) in->gfid;
    LOGDBG("getting file size for gfid=%d", gfid);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };

    size_t filesize = 0;
    ret = unifyfs_fops_filesize(&ctx, gfid, &filesize);
    if (ret != UNIFYFS_PENDING) {
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("unifyfs_fops_filesize() failed");
        }
        
        out->filesize = (hg_size_t) filesize;
        out->ret = (int32_t) ret;
        
        /* send rpc response and cleanup request state */
        sync_respond_client(creq, rpc_name);
    }
    // else, some other thread will respond when pending req finishes
}

void process_client_fsync_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_fsync";
    unifyfs_fsync_in_t* in = creq->req_state->inputs;
    unifyfs_fsync_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));
    
    int gfid = (int) in->gfid;
    LOGINFO("syncing gfid=%d", gfid);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };
    ret = unifyfs_fops_fsync(&ctx, gfid);
    if (ret != UNIFYFS_SUCCESS) {
        LOGERR("unifyfs_fops_fsync() failed");
        out->ret = (int32_t) ret;
    
        /* send rpc response and cleanup request state */
        sync_respond_client(creq, rpc_name);
        
    }
    // else, some other thread will update the extent metadata
    //       and respond when the sync has completed
}

void process_client_mread_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_mread";
    unifyfs_mread_in_t* in = creq->req_state->inputs;
    unifyfs_mread_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    int mread_id = in->mread_id;
    size_t read_count = in->read_count;
    LOGDBG("processing mread[%d] with %zu requests", mread_id, read_count);

    /* allocate buffer to hold array of read requests */
    void* buffer = pull_margo_bulk(creq->req_state->handle,
                                   in->bulk_extents, in->bulk_size, NULL);
    if (NULL == buffer) {
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        unifyfs_fops_ctx_t ctx = {
            .app_id    = in->app_id,
            .client_id = in->client_id,
            .mread_id  = mread_id
        };
        ret = unifyfs_fops_mread(&ctx, read_count, buffer);
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("unifyfs_fops_read() failed");
        }
        free(buffer);
    }

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
}

void process_client_read_extent_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_read_extent";
    unifyfs_read_extent_in_t* in = creq->req_state->inputs;
    unifyfs_read_extent_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    unifyfs_fops_ctx_t ctx = {
        .app_id    = in->app_id,
        .client_id = in->client_id,
        .mread_id  = 0
    };
    unifyfs_extent_t ext = in->extent;
    size_t nbytes = 0;
    size_t cover_begin = (size_t) -1;
    size_t cover_end   = (size_t) -1;
    void* buffer = calloc(1, ext.length);
    if (NULL == buffer) {
        LOGERR("failed to allocate server read buffer");
        ret = ENOMEM;
    } else {
        ret = unifyfs_fops_read(&ctx, ext.gfid, ext.offset,
                                ext.length, buffer,
                                &nbytes, &cover_begin, &cover_end);
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("unifyfs_fops_read() failed");
        } else {
            // push extent buffer to client bulk
            hg_bulk_t client_bulk = in->bulk_extent;
            ret = push_margo_bulk(creq->req_state->handle,
                                  client_bulk, 0,
                                  nbytes, buffer);
            if (ret != UNIFYFS_SUCCESS) {
                nbytes = 0;
                LOGERR("failed to push data to client buffer");
            }
        }
        free(buffer);
    }

    out->ret = (int32_t) ret;
    out->bytes_read = (hg_size_t) nbytes;
    out->coverage_begin = (hg_size_t) cover_begin;
    out->coverage_end = (hg_size_t) cover_end;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
}

void process_client_truncate_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_truncate";
    unifyfs_truncate_in_t* in = creq->req_state->inputs;
    unifyfs_truncate_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    int gfid = in->gfid;
    size_t filesize = in->filesize;
    LOGDBG("setting file size for gfid=%d to sz=%zu", gfid, filesize);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };
    ret = unifyfs_fops_truncate(&ctx, gfid, filesize);
    if (ret != UNIFYFS_PENDING) {
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("unifyfs_fops_truncate() failed");
        }

        out->ret = (int32_t) ret;

        /* send rpc response and cleanup request state */
        sync_respond_client(creq, rpc_name);
    }
    // else, some other thread will respond when pending req finishes
}

void process_client_laminate_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_laminate";
    unifyfs_laminate_in_t* in = creq->req_state->inputs;
    unifyfs_laminate_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));
    
    int gfid = in->gfid;
    LOGDBG("laminating gfid=%d", gfid);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };
    ret = unifyfs_fops_laminate(&ctx, gfid);
    if (ret != UNIFYFS_PENDING) {
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("unifyfs_fops_laminate() failed");
        }

        out->ret = (int32_t) ret;

        /* send rpc response and cleanup request state */
        sync_respond_client(creq, rpc_name);
    }
    // else, some other thread will respond when pending req finishes
}

void process_client_unlink_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_unlink";
    unifyfs_unlink_in_t* in = creq->req_state->inputs;
    unifyfs_unlink_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));
    
    int gfid = in->gfid;
    LOGDBG("unlinking gfid=%d", gfid);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };
    ret = unifyfs_fops_unlink(&ctx, gfid);
    if (ret != UNIFYFS_SUCCESS) {
        LOGERR("unifyfs_fops_unlink() failed");
    }

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
    
}

void process_client_transfer_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_transfer";
    unifyfs_transfer_in_t* in = creq->req_state->inputs;
    unifyfs_transfer_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    int transfer_id = in->transfer_id;
    int gfid = in->gfid;
    int mode = (in->mode == 1 ? SERVER_TRANSFER_MODE_LOCAL
                              : SERVER_TRANSFER_MODE_OWNER);
    const char* dest_file = strdup(in->dst_file);

    LOGDBG("transferring gfid=%d to file %s", gfid, dest_file);

    unifyfs_fops_ctx_t ctx = {
        .client_req = creq,
        .app_id     = in->app_id,
        .client_id  = in->client_id,
    };
    ret = unifyfs_fops_transfer(&ctx, transfer_id, gfid, mode, dest_file);
    if (ret != UNIFYFS_PENDING) {
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("unifyfs_fops_transfer() failed");
        }

        out->ret = (int32_t) ret;

        /* send rpc response and cleanup request state */
        sync_respond_client(creq, rpc_name);
    }
    // else, some other thread will respond when pending req finishes
}

void process_client_gfids_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;
    hg_return_t hret;

    const char* rpc_name = "unifyfs_get_gfids";
    unifyfs_get_gfids_in_t* in = creq->req_state->inputs;
    unifyfs_get_gfids_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* Submit a broadcast metaget_all request and wait for it to complete. */
    // TODO: This is horribly wasteful! We fetch the full metadata for all
    //       files, but only use the gfids. The client must then issue
    //       a separate metaget request for each gfid.
    unifyfs_file_attr_t* file_attrs = NULL;
    int* new_gfid_list = NULL;
    int num_file_attrs = 0;
    ret = unifyfs_invoke_broadcast_metaget_all(&file_attrs,
                                               &num_file_attrs);
    if (UNIFYFS_SUCCESS != ret) {
        LOGERR("unifyfs_invoke_broadcast_metaget_all() failed");
    } else if (num_file_attrs > 0) {
        // Package all the gfids up into one list
        new_gfid_list = (int*) calloc(num_file_attrs, sizeof(int));
        if (NULL != new_gfid_list) {
            /* initialize bulk handle for the gfid_list */
            hg_bulk_t bulk_gfids;
            hg_size_t sizes[1] = { num_file_attrs * sizeof(int) };
            void* ptrs[1] = { (void*)new_gfid_list };
            hret = margo_bulk_create(unifyfsd_rpc_context->shm_mid,
                                     1, ptrs, sizes,
                                     HG_BULK_READ_ONLY, &bulk_gfids);
            if (hret != HG_SUCCESS) {
                LOGDBG("margo_bulk_create() failed - %s",
                       HG_Error_to_string(hret));
                free(new_gfid_list);
                ret = UNIFYFS_ERROR_MARGO;
            } else {
                for (int i=0; i < num_file_attrs; i++) {
                    new_gfid_list[i] = file_attrs[i].gfid;
                }
                out->bulk_gfids = bulk_gfids;
                creq->req_state->bulk = bulk_gfids;
            }
        } else {
            ret = ENOMEM;
        }
    }

    out->ret = (int32_t) ret;
    out->num_gfids = num_file_attrs;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
    

    if (NULL != new_gfid_list)
        free(new_gfid_list);

    if (NULL != file_attrs)
        free(file_attrs);
}

void process_client_node_local_extents_rpc(client_rpc_req_t* creq)
{
    int ret = UNIFYFS_SUCCESS;

    const char* rpc_name = "unifyfs_node_local_extents_get";
    unifyfs_node_local_extents_get_in_t* in = creq->req_state->inputs;
    unifyfs_node_local_extents_get_out_t* out = creq->req_state->outputs;
    assert((in != NULL) && (out != NULL));

    /* allocate buffer to hold array of read requests */
    unifyfs_extent_t* extents = NULL;
    size_t num_req = in->num_req;
    hg_size_t bulk_size = in->bulk_size;
    void* buffer = pull_margo_bulk(creq->req_state->handle,
                                   in->bulk_data, bulk_size, NULL);
    if (NULL == buffer) {
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        extents = (unifyfs_extent_t*) buffer;
    }

    chunk_list_t* elist_head = NULL;
    chunk_list_t* elist_cur = NULL;
    size_t total_chunks = 0;
    if (num_req > 0 && (NULL != extents)) {
        /* find chunks for each input extent */
        elist_head = calloc(1, sizeof(chunk_list_t));
        elist_cur = elist_head;
        bool grow_list = false;
        for (int i=0; i < num_req; ++i) {
            LOGDBG("getting node local extent for gfid=%d",
                   extents[i].gfid);
            unsigned int n_chunks = 0;
            unifyfs_data_chunk_t* chunks = NULL;
            int rc = unifyfs_invoke_find_extents_rpc(extents[i].gfid, 1,
                                                     &extents[i],
                                                     &n_chunks, &chunks);
            if (0 == rc) {
                if (grow_list) {
                    elist_cur->next = calloc(1, sizeof(chunk_list_t));
                    elist_cur = elist_cur->next;
                }
                for (int j = 0; j < n_chunks; ++j) {
                    elist_cur->chunk = chunks[j];
                    if (j < n_chunks - 1) {
                        elist_cur->next = calloc(1, sizeof(chunk_list_t));
                        elist_cur = elist_cur->next;
                    } else {
                        elist_cur->next = NULL;
                    }
                }
                grow_list = true;
                total_chunks += n_chunks;
                free(chunks);
            }
        }
        free(extents);
    }
    
    unifyfs_data_chunk_t* chunks_buffer = NULL;
    size_t chunks_size = total_chunks * sizeof(unifyfs_data_chunk_t);
    if (total_chunks > 0) {
        /* convert intermediate list to array of unifyfs_data_chunk_t */
        chunks_buffer = calloc(1, chunks_size);
        elist_cur = elist_head;
        for (int i = 0; i < total_chunks; ++i) {
            chunks_buffer[i] = elist_cur->chunk;
            chunk_list_t* elist_tmp = elist_cur;
            elist_cur = elist_cur->next;
            free(elist_tmp);
        }

        hg_bulk_t bulk_extents;
        hg_return_t hret = margo_bulk_create(unifyfsd_rpc_context->shm_mid, 1,
                                             (void**) &chunks_buffer,
                                             &chunks_size,
                                             HG_BULK_READ_ONLY, &bulk_extents);
        if (hret == HG_SUCCESS) {
            out->chunk_count = total_chunks;
            out->bulk_size = chunks_size;
            out->bulk_data = bulk_extents;
            creq->req_state->bulk = bulk_extents;
        } else {
            LOGDBG("margo_bulk_create() failed - %s",
                   HG_Error_to_string(hret));
            ret = UNIFYFS_ERROR_MARGO;
            out->chunk_count = 0;
            out->bulk_size = 0;
            out->bulk_data = HG_BULK_NULL;
        }
    }

    out->ret = (int32_t) ret;

    /* send rpc response and cleanup request state */
    sync_respond_client(creq, rpc_name);
    
    if (NULL != chunks_buffer)
        free(chunks_buffer);
}

/* BEGIN MARGO CLIENT-SERVER RPC HANDLER FUNCTIONS */

/* called by client to register with the server, client provides a
 * structure of values on input, some of which specify global
 * values across all clients in the app_id, and some of which are
 * specific to the client process,
 *
 * server creates a structure for the given app_id (if needed),
 * and then fills in a set of values for the particular client,
 *
 * server attaches to client shared memory regions, opens files
 * holding spill over data, and launchers request manager for
 * client */
static void unifyfs_mount_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_MOUNT, handle,
                                  sizeof(unifyfs_mount_in_t),
                                  sizeof(unifyfs_mount_out_t));
    if (NULL == creq) {
        unifyfs_mount_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_mount_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_mount_rpc)

/* server attaches to client's shared memory region, opens file
 * holding spillover data */
static void unifyfs_attach_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_ATTACH, handle,
                                  sizeof(unifyfs_attach_in_t),
                                  sizeof(unifyfs_attach_out_t));
    if (NULL == creq) {
        unifyfs_attach_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_attach_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_attach_rpc)

static void unifyfs_unmount_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_UNMOUNT, handle,
                                  sizeof(unifyfs_unmount_in_t),
                                  sizeof(unifyfs_unmount_out_t));
    if (NULL == creq) {
        unifyfs_unmount_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_unmount_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_unmount_rpc)

/* returns file meta data including file size and file name
 * given a global file id */
static void unifyfs_metaget_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_METAGET, handle,
                                  sizeof(unifyfs_metaget_in_t),
                                  sizeof(unifyfs_metaget_out_t));
    if (NULL == creq) {
        unifyfs_metaget_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_metaget_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_metaget_rpc)

/* given a global file id and a file name,
 * record key/value entry for this file */
static void unifyfs_metaset_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_METASET, handle,
                                  sizeof(unifyfs_metaset_in_t),
                                  sizeof(unifyfs_metaset_out_t));
    if (NULL == creq) {
        unifyfs_metaset_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_metaset_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_metaset_rpc)

/* given a global file id and client identified by (app_id, client_id) as
 * input, read the write extents for the file from the shared memory index
 * and update its global metadata */
static void unifyfs_fsync_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_SYNC, handle,
                                  sizeof(unifyfs_fsync_in_t),
                                  sizeof(unifyfs_fsync_out_t));
    if (NULL == creq) {
        unifyfs_filesize_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_fsync_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_fsync_rpc)

/* given an app_id, client_id, global file id,
 * return current file size */
static void unifyfs_filesize_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_FILESIZE, handle,
                                  sizeof(unifyfs_filesize_in_t),
                                  sizeof(unifyfs_filesize_out_t));
    if (NULL == creq) {
        unifyfs_filesize_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_filesize_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_filesize_rpc)

/* given an app_id, client_id, global file id, transfer mode
 * and destination file, transfer data to that file */
static void unifyfs_transfer_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_TRANSFER, handle,
                                  sizeof(unifyfs_transfer_in_t),
                                  sizeof(unifyfs_transfer_out_t));
    if (NULL == creq) {
        unifyfs_transfer_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_transfer_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_transfer_rpc)

/* given an app_id, client_id, global file id,
 * and file size, truncate file to that size */
static void unifyfs_truncate_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_TRUNCATE, handle,
                                  sizeof(unifyfs_truncate_in_t),
                                  sizeof(unifyfs_truncate_out_t));
    if (NULL == creq) {
        unifyfs_truncate_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_truncate_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_truncate_rpc)

/* given an app_id, client_id, and global file id,
 * remove file from system */
static void unifyfs_unlink_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_UNLINK, handle,
                                  sizeof(unifyfs_unlink_in_t),
                                  sizeof(unifyfs_unlink_out_t));
    if (NULL == creq) {
        unifyfs_unlink_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_unlink_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_unlink_rpc)

/* given an app_id, client_id, and global file id,
 * laminate file */
static void unifyfs_laminate_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_LAMINATE, handle,
                                  sizeof(unifyfs_laminate_in_t),
                                  sizeof(unifyfs_laminate_out_t));
    if (NULL == creq) {
        unifyfs_laminate_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_laminate_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_laminate_rpc)


/* given (mread_id, app_id, client_id) and count of read requests,
 * followed by a bulk data array of read extents (unifyfs_extent_t),
 * initiate read requests for data. */
static void unifyfs_mread_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_MREAD, handle,
                                  sizeof(unifyfs_mread_in_t),
                                  sizeof(unifyfs_mread_out_t));
    if (NULL == creq) {
        unifyfs_mread_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_mread_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_mread_rpc)

/* given extent (gfid, offset, length) and bulk representing user buffer,
 * initiate chunk read requests for data. */
static void unifyfs_read_extent_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_READ_EXTENT, handle,
                                  sizeof(unifyfs_read_extent_in_t),
                                  sizeof(unifyfs_read_extent_out_t));
    if (NULL == creq) {
        unifyfs_read_extent_out_t out;
        out.ret = (int32_t) ENOMEM;
        out.bytes_read = 0;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_read_extent_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_read_extent_rpc)

/* Request a list of all the GFIDs the server knows about */
static void unifyfs_get_gfids_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(UNIFYFS_CLIENT_RPC_GET_GFIDS, handle,
                                  sizeof(unifyfs_get_gfids_in_t),
                                  sizeof(unifyfs_get_gfids_out_t));
    if (NULL == creq) {
        unifyfs_get_gfids_out_t out;
        out.ret = (int32_t) ENOMEM;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_gfids_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_get_gfids_rpc)

/* returns file extents from node local server
 * given a global file id */
static void unifyfs_node_local_extents_get_rpc(hg_handle_t handle)
{
    /* create client rpc state */
    client_rpc_req_t* creq =
        allocate_client_rpc_state(
            UNIFYFS_CLIENT_RPC_NODE_LOCAL_EXTENTS_GET, handle,
            sizeof(unifyfs_node_local_extents_get_in_t),
            sizeof(unifyfs_node_local_extents_get_out_t));
    if (NULL == creq) {
        unifyfs_node_local_extents_get_out_t out;
        out.ret = (int32_t) ENOMEM;
        out.chunk_count = 0;
        hg_return_t hret = margo_respond(handle, &out);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_respond() failed - %s", HG_Error_to_string(hret));
        }
        return;
    }
    process_client_node_local_extents_rpc(creq);
}
DEFINE_MARGO_RPC_HANDLER(unifyfs_node_local_extents_get_rpc)
