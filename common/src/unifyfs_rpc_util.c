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

#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <config.h>
#include <margo.h>
#include <assert.h>
#include "unifyfs_log.h"
#include "unifyfs_keyval.h"
#include "unifyfs_rpc_util.h"

#define LOCAL_RPC_ADDR_FILE "/tmp/unifyfsd.margo-shm"

/* publishes client-server RPC address */
void rpc_publish_local_server_addr(const char* addr)
{
    LOGDBG("publishing client-server rpc address '%s'", addr);

    // publish client-server margo address
    unifyfs_keyval_publish_local(key_unifyfsd_margo_shm, addr);

    /* write server address to local file for client to read */
    FILE* fp = fopen(LOCAL_RPC_ADDR_FILE, "w+");
    if (fp != NULL) {
        fprintf(fp, "%s", addr);
        fclose(fp);
    } else {
        LOGERR("Error writing server rpc addr file " LOCAL_RPC_ADDR_FILE);
    }
}

/* publishes server-server RPC address */
void rpc_publish_remote_server_addr(const char* addr)
{
    LOGDBG("publishing server-server rpc address '%s'", addr);

    // publish client-server margo address
    unifyfs_keyval_publish_remote(key_unifyfsd_margo_svr, addr);
}

/* lookup address of server, returns NULL if server address is not found,
 * otherwise returns server address in newly allocated string that caller
 * must free */
char* rpc_lookup_local_server_addr(void)
{
    /* returns NULL if we can't find server address */
    char* addr = NULL;
    char* valstr = NULL;

    // lookup client-server margo address
    if (0 == unifyfs_keyval_lookup_local(key_unifyfsd_margo_shm, &valstr)) {
        addr = strdup(valstr);
        free(valstr);
    }

    if (NULL == addr) {
        /* read server address from local file */
        FILE* fp = fopen(LOCAL_RPC_ADDR_FILE, "r");
        if (fp != NULL) {
            char addr_string[256];
            memset(addr_string, 0, sizeof(addr_string));
            if (1 == fscanf(fp, "%255s", addr_string)) {
                addr = strdup(addr_string);
            }
            fclose(fp);
        }
    }

    /* print server address (debugging) */
    if (NULL != addr) {
        LOGDBG("found local server rpc address '%s'", addr);
    }
    return addr;
}

/* lookup address of server, returns NULL if server address is not found,
 * otherwise returns server address in newly allocated string that caller
 * must free */
char* rpc_lookup_remote_server_addr(int srv_rank)
{
    /* returns NULL if we can't find server address */
    char* addr = NULL;
    char* valstr = NULL;

    // lookup server-server margo address
    if (0 == unifyfs_keyval_lookup_remote(srv_rank, key_unifyfsd_margo_svr,
                                          &valstr)) {
        addr = strdup(valstr);
        free(valstr);
    }

    /* print sserver address (debugging) */
    if (NULL != addr) {
        LOGDBG("found server %d rpc address '%s'", srv_rank, addr);
    }
    return addr;
}

/* remove local server RPC address file */
void rpc_clean_local_server_addr(void)
{
    int rc = unlink(LOCAL_RPC_ADDR_FILE);
    if (rc != 0) {
        int err = errno;
        if (err != ENOENT) {
            LOGERR("Error (%s) removing local server rpc addr file "
                   LOCAL_RPC_ADDR_FILE, strerror(err));
        }
    }
}


/* Given a margo instance ID (mid) and hg_addr, return its corresponding
 * address as a newly allocated string to be freed by caller.
 * Returns NULL on error. */
char*
get_margo_addr_str(margo_instance_id mid,
                   hg_addr_t maddr)
{
    /* convert margo address to a string */
    char addr_string[128];
    hg_size_t addr_string_sz = sizeof(addr_string);
    hg_return_t hret = margo_addr_to_string(mid, addr_string,
                                            &addr_string_sz, maddr);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_addr_to_string() failed - %s",
               HG_Error_to_string(hret));
        return NULL;
    }

    /* return address in newly allocated string */
    char* addr = strdup(addr_string);
    return addr;
}

rpc_state*
create_rpc_request(hg_id_t rpc_id,
                   margo_instance_id mid,
                   hg_addr_t maddr,
                   void* input, size_t input_sz,
                   void* output, size_t output_sz)
{
    rpc_state* new_rpc = (rpc_state*) calloc(1, sizeof(rpc_state));
    if (NULL != new_rpc) {
        /* create handle for given rpc id */
        hg_handle_t handle = HG_HANDLE_NULL;
        hg_return_t hret = margo_create(mid, maddr, rpc_id, &handle);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_create() failed - %s",
                   HG_Error_to_string(hret));
        } else {
            LOGDBG("created state for request rpc(%p) with handle(%p)",
                   new_rpc, handle);

            new_rpc->initiator = 1;
            new_rpc->handle = handle;
            new_rpc->mid = mid;
            new_rpc->rpc_id = rpc_id;

            if (NULL != input) {
                new_rpc->inputs = input;
            } else if (0 != input_sz) { /* allocate it */
                new_rpc->inputs = calloc(1, input_sz);
                if (NULL != new_rpc->inputs)
                    new_rpc->inputs_sz = input_sz;
            }

            if (NULL != output) {
                new_rpc->outputs = output;
            } else if (0 != output_sz) { /* allocate it */
                new_rpc->outputs = calloc(1, output_sz);
                if (NULL != new_rpc->outputs)
                    new_rpc->outputs_sz = output_sz;
            }
        }
    }
    return new_rpc;
}

rpc_state*
create_rpc_response(hg_handle_t handle,
                    void* input,
                    void* output, size_t output_sz)
{
    rpc_state* new_rpc = (rpc_state*) calloc(1, sizeof(rpc_state));
    if (NULL != new_rpc) {
        LOGDBG("created state for response rpc(%p) with handle(%p)",
               new_rpc, handle);
        
        new_rpc->initiator = 0;
        new_rpc->handle = handle;

        if (HG_HANDLE_NULL != handle) {
            margo_instance_id mid = margo_hg_handle_get_instance(handle);
            assert(mid != MARGO_INSTANCE_NULL);
            new_rpc->mid = mid;

            const struct hg_info* hgi = margo_get_info(handle);
            assert(hgi);
            new_rpc->rpc_id = hgi->id;
        }
        
        if (NULL != input) {
            new_rpc->inputs = input;
            new_rpc->have_input = 1;
        } 

        if (NULL != output) {
            new_rpc->outputs = output;
        } else if (0 != output_sz) { /* allocate it */
            new_rpc->outputs = calloc(1, output_sz);
            if (NULL != new_rpc->outputs)
                new_rpc->outputs_sz = output_sz;
        }
    }
    return new_rpc;
}

int cleanup_rpc_state(rpc_state* rpc)
{
    if (NULL == rpc)
        return EINVAL;

    int ret = UNIFYFS_SUCCESS;
    hg_return_t hret;

    if (HG_HANDLE_NULL != rpc->handle) {

        LOGDBG("cleaning state for rpc(%p) with handle(%p)",
               rpc, rpc->handle);

        if (!rpc->initiator && rpc->have_input) {
            LOGDBG("calling margo_free_input() for rpc(%p)", rpc);
            hret = margo_free_input(rpc->handle, rpc->inputs);
            if (hret != HG_SUCCESS)
                LOGERR("margo_free_input() failed - %s",
                       HG_Error_to_string(hret));
        }

        if (rpc->initiator && rpc->have_output) {
            LOGDBG("calling margo_free_output() for rpc(%p)", rpc);
            hret = margo_free_output(rpc->handle, rpc->outputs);
            if (hret != HG_SUCCESS)
                LOGERR("margo_free_output() failed - %s",
                       HG_Error_to_string(hret));
        }

        margo_destroy(rpc->handle);
    }

    if (HG_BULK_NULL != rpc->bulk) {
        LOGDBG("calling margo_bulk_free(%p) for rpc(%p)", rpc->bulk, rpc);
        margo_bulk_free(rpc->bulk);
    }    

    if ((NULL != rpc->inputs) && (0 != rpc->inputs_sz)) {
        /* free since we allocated it */
        LOGDBG("freeing input args for rpc(%p)", rpc);
        free(rpc->inputs);
    }

    if ((NULL != rpc->outputs) && (0 != rpc->outputs_sz)) {
        /* free since we allocated it */
        LOGDBG("freeing output args for rpc(%p)", rpc);
        free(rpc->outputs);
    }

    free(rpc);
    return ret;
}

int sync_rpc_request(rpc_state* rpc,
                     int timeout_msec,
                     int retry)
{
    if (NULL == rpc)
        return EINVAL;

    int ret = UNIFYFS_SUCCESS;
    int done = 0;
    double timeout_ms = 1.0 * timeout_msec;
    do {
        hg_return_t hret = margo_forward_timed(rpc->handle, rpc->inputs,
                                               timeout_ms);
        if (hret == HG_TIMEOUT) { /* timed-out */
            LOGINFO("margo_forward_timed(%p) timed-out",
                    rpc->handle);
            if (!retry) {
                done = 1;
                ret = UNIFYFS_ERROR_TIMEOUT;
            } else {
                retry -= 1;
            }
        } else if (hret != HG_SUCCESS) { /* other forwarding error */
            LOGERR("margo_forward_timed(%p) failed - %s",
                   rpc->handle, HG_Error_to_string(hret));
            ret = UNIFYFS_ERROR_MARGO;
            done = 1;
        } else { /* success */
            LOGDBG("margo_forward_timed(%p) succeeded",
                   rpc->handle);
            if (NULL != rpc->outputs) {
                hret = margo_get_output(rpc->handle, rpc->outputs);
                if (hret != HG_SUCCESS) {
                    LOGERR("margo_get_output(%p) failed - %s",
                           rpc->handle, HG_Error_to_string(hret));
                    ret = UNIFYFS_ERROR_MARGO;
                } else {
                    rpc->have_output = 1;
                }
            }
            done = 1;
        }
    } while (!done);
    return ret;
}

int sync_rpc_response(rpc_state* rpc,
                      int retry)
{
    if (NULL == rpc)
        return EINVAL;

    int ret = UNIFYFS_SUCCESS;
    int done = 0;
    do {
        hg_return_t hret = margo_respond(rpc->handle, rpc->outputs);
        if (hret != HG_SUCCESS) { /* response error */
            LOGERR("margo_respond(%p) failed - %s",
                   rpc->handle, HG_Error_to_string(hret));
            if (!retry) {
                ret = UNIFYFS_ERROR_MARGO;
                done = 1;
            } else {
                retry -= 1;
            }
        } else { /* success */
            done = 1;
        }
    } while (!done);
    return ret;
}

int async_rpc_request(rpc_state* rpc,
                      int timeout_msec)
{
    if (NULL == rpc)
        return EINVAL;

    int ret = UNIFYFS_SUCCESS;
    double timeout_ms = 1.0 * timeout_msec;
    margo_request mreq;
    hg_return_t hret = margo_iforward_timed(rpc->handle, rpc->inputs,
                                            timeout_ms, &mreq);
    if (hret != HG_SUCCESS) { /* other forwarding error */
        LOGERR("margo_iforward_timed(%p) failed - %s",
               rpc->handle, HG_Error_to_string(hret));
        ret = UNIFYFS_ERROR_MARGO;
    } else { /* success */
        LOGDBG("margo_iforward_timed(%p) successful -> margo_req(%p)",
               rpc->handle, mreq);
        rpc->mreq = mreq;
    }
    return ret;
}

int async_rpc_request_finish(rpc_state* rpc)
{
    if (NULL == rpc)
        return EINVAL;

    int ret = UNIFYFS_SUCCESS;
    hg_return_t hret = margo_wait(rpc->mreq);
    if (hret != HG_SUCCESS) { /* other forwarding error */
        LOGERR("margo_wait(%p) failed - %s",
               rpc->mreq, HG_Error_to_string(hret));
        ret = UNIFYFS_ERROR_MARGO;
    } else {
        if (NULL != rpc->outputs) {
            hret = margo_get_output(rpc->handle, rpc->outputs);
            if (hret != HG_SUCCESS) {
                LOGERR("margo_get_output(%p) failed - %s",
                       rpc->handle, HG_Error_to_string(hret));
                ret = UNIFYFS_ERROR_MARGO;
            } else {
                rpc->have_output = 1;
            }
        }
    }
    return ret;
}

int async_rpc_response(rpc_state* rpc,
                       int retry)
{
    if (NULL == rpc)
        return EINVAL;

    int ret = UNIFYFS_SUCCESS;
    int done = 0;
    do {
        margo_request mreq;
        hg_return_t hret = margo_irespond(rpc->handle, rpc->outputs, &mreq);
        if (hret != HG_SUCCESS) { /* response error */
            LOGERR("margo_irespond(%p) failed - %s",
                   rpc->handle, HG_Error_to_string(hret));
            if (!retry) {
                ret = UNIFYFS_ERROR_MARGO;
                done = 1;
            } else {
                retry -= 1;
            }
        } else { /* success */
            rpc->mreq = mreq;
            done = 1;
        }
    } while (!done);
    return ret;
}

/* Wait on the margo_request for the async RCP */
int async_rpc_response_finish(rpc_state* rpc)
{
    if (NULL == rpc)
        return EINVAL;

    int ret = UNIFYFS_SUCCESS;
    hg_return_t hret = margo_wait(rpc->mreq);
    if (hret != HG_SUCCESS) { /* other forwarding error */
        LOGERR("margo_wait(%p) failed - %s",
               rpc->mreq, HG_Error_to_string(hret));
        ret = UNIFYFS_ERROR_MARGO;
    }
    return ret;
}

/* Use passed bulk handle to pull data into a newly allocated buffer.
 * If local_bulk is not NULL, will set to local bulk handle on success.
 * Returns bulk buffer, or NULL on failure. */
void* pull_margo_bulk(hg_handle_t rpc_hdl,
                      hg_bulk_t bulk_remote,
                      hg_size_t bulk_sz,
                      hg_bulk_t* local_bulk)
{
    if (0 == bulk_sz) {
        return NULL;
    }

    size_t sz = (size_t) bulk_sz;
    void* buffer = malloc(sz);
    if (NULL == buffer) {
        LOGERR("failed to allocate buffer(sz=%zu) for bulk transfer", sz);
        return NULL;
    }

    /* get mercury info to set up bulk transfer */
    const struct hg_info* hgi = margo_get_info(rpc_hdl);
    assert(hgi);
    margo_instance_id mid = margo_hg_handle_get_instance(rpc_hdl);
    assert(mid != MARGO_INSTANCE_NULL);

    /* register local target buffer for bulk access */
    hg_bulk_t bulk_local;
    hg_return_t hret = margo_bulk_create(mid, 1, &buffer, &bulk_sz,
                                         HG_BULK_READWRITE, &bulk_local);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s",
               HG_Error_to_string(hret));
        free(buffer);
        return NULL;
    }

    /* execute the transfer to pull data from remote side
     * into our local buffer.
     *
     * NOTE: mercury/margo bulk transfer does not check the maximum
     * transfer size that the underlying transport supports, and a
     * large bulk transfer may result in failure. */
    int i = 0;
    hg_size_t max_bulk = UNIFYFS_SERVER_MAX_BULK_TX_SIZE;
    hg_size_t remain = bulk_sz;
    do {
        hg_size_t offset = i * max_bulk;
        hg_size_t len = (remain < max_bulk) ? remain : max_bulk;
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, hgi->addr,
                                   bulk_remote, offset,
                                   bulk_local, offset, len);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_bulk_transfer(buf_offset=%zu, len=%zu) failed - %s",
                   (size_t)offset, (size_t)len, HG_Error_to_string(hret));
            break;
        }
        remain -= len;
        i++;
    } while (remain > 0);

    if (hret == HG_SUCCESS) {
        LOGDBG("successful bulk transfer (%zu bytes)", bulk_sz);
        if (local_bulk != NULL) {
            *local_bulk = bulk_local;
        } else {
            /* deregister our bulk transfer buffer */
            margo_bulk_free(bulk_local);
        }
        return buffer;
    } else {
        LOGERR("failed bulk transfer (transferred %zu of %zu bytes) - %s",
               (bulk_sz - remain), bulk_sz, HG_Error_to_string(hret));
        free(buffer);
        return NULL;
    }
}

/* push data from local buffer to remote bulk */
int push_margo_bulk(hg_handle_t rpc_hdl,
                    hg_bulk_t bulk_remote,
                    hg_size_t bulk_out_offset,
                    hg_size_t buf_sz,
                    void* local_buf)
{
    if (0 == buf_sz) {
        return UNIFYFS_SUCCESS;
    }

    if (NULL == local_buf) {
        return EINVAL;
    }

    /* get mercury info to set up bulk transfer */
    const struct hg_info* hgi = margo_get_info(rpc_hdl);
    assert(hgi);
    margo_instance_id mid = margo_hg_handle_get_instance(rpc_hdl);
    assert(mid != MARGO_INSTANCE_NULL);

    /* register local source buffer for bulk access */
    hg_bulk_t bulk_local;
    hg_return_t hret = margo_bulk_create(mid, 1, &local_buf, &buf_sz,
                                         HG_BULK_READ_ONLY, &bulk_local);
    if (hret != HG_SUCCESS) {
        LOGERR("margo_bulk_create() failed - %s",
               HG_Error_to_string(hret));
        return UNIFYFS_ERROR_MARGO;
    }
    
    /* execute the transfer to push data from local buffer
     * into remote buffer.
     *
     * NOTE: mercury/margo bulk transfer does not check the maximum
     * transfer size that the underlying transport supports, and a
     * large bulk transfer may result in failure. */
    int i = 0;
    hg_size_t max_bulk = UNIFYFS_SERVER_MAX_BULK_TX_SIZE;
    hg_size_t remain = buf_sz;
    do {
        hg_size_t buf_offset = i * max_bulk;
        hg_size_t len = (remain < max_bulk) ? remain : max_bulk;
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr,
                                   bulk_remote, bulk_out_offset + buf_offset,
                                   bulk_local, buf_offset, len);
        if (hret != HG_SUCCESS) {
            LOGERR("margo_bulk_transfer(buf_offset=%zu, len=%zu) failed - %s",
                   (size_t)buf_offset, (size_t)len, HG_Error_to_string(hret));
            break;
        }
        remain -= len;
        i++;
    } while (remain > 0);

    if (hret == HG_SUCCESS) {
        LOGDBG("successful bulk transfer (%zu bytes)", buf_sz);
        
        /* deregister our bulk transfer buffer */
        margo_bulk_free(bulk_local);
    } else {
        LOGERR("failed bulk transfer (transferred %zu of %zu bytes) - %s",
               (buf_sz - remain), buf_sz, HG_Error_to_string(hret));
        return UNIFYFS_ERROR_MARGO;
    }

    return UNIFYFS_SUCCESS;
}