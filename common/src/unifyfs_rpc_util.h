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

#ifndef UNIFYFS_UTIL_H
#define UNIFYFS_UTIL_H

#include <mercury_types.h>
#include "unifyfs_rpc_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// ========== publish/lookup of Mercury RPC addresses ==========

/* publish the address of the server */
void rpc_publish_local_server_addr(const char* addr);
void rpc_publish_remote_server_addr(const char* addr);

/* lookup address of server */
char* rpc_lookup_local_server_addr(void);
char* rpc_lookup_remote_server_addr(int srv_rank);

/* remove server rpc address file */
void rpc_clean_local_server_addr(void);

// ========== Margo RPC helper functions ==========

char* get_margo_addr_str(margo_instance_id mid,
                         hg_addr_t maddr);

rpc_state* create_rpc_request(hg_id_t rpc_id,
                              margo_instance_id mid,
                              hg_addr_t maddr,
                              void* input, size_t input_sz,
                              void* output, size_t output_sz);

rpc_state* create_rpc_response(hg_handle_t handle,
                               void* input,
                               void* output, size_t output_sz);

int cleanup_rpc_state(rpc_state* rpc);

int sync_rpc_request(rpc_state* rpc,
                     int timeout_msec,
                     int retry);

int sync_rpc_response(rpc_state* rpc,
                      int retry);

int async_rpc_request(rpc_state* rpc,
                      int timeout_msec);
int async_rpc_request_finish(rpc_state* rpc,
                             int timeout_msec,
                             int retry);

int async_rpc_response(rpc_state* rpc,
                       int retry);
int async_rpc_response_finish(rpc_state* rpc);

/* use passed bulk handle to pull data into a newly allocated buffer.
 * returns buffer, or NULL on failure. */
void* pull_margo_bulk(hg_handle_t rpc_hdl,
                      hg_bulk_t bulk_in,
                      hg_size_t bulk_sz,
                      hg_bulk_t* local_bulk);

/* push data from local buffer to offset within passed bulk handle.
 * returns buffer, or NULL on failure. */
int push_margo_bulk(hg_handle_t rpc_hdl,
                    hg_bulk_t bulk_out,
                    hg_size_t bulk_out_offset,
                    hg_size_t buf_sz,
                    void* local_buf);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // UNIFYFS_UTIL_H

