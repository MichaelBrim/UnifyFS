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

#ifndef __UNIFYFS_RPC_TYPES_H
#define __UNIFYFS_RPC_TYPES_H

#include <margo.h>
#include <mercury_proc_string.h>
#include <time.h>

#include "unifyfs_meta.h"

/* Common state necessary to track and cleanup Margo RPCs */
typedef struct rpc_state {
    margo_instance_id mid; // which instance (client|server) this RPC uses
    hg_id_t rpc_id;        // registered RPC id
    hg_handle_t handle;    // handle passed to RPC handler func
    margo_request mreq;    // request id for non-blocking RPCs
    
    void* inputs;      // pointer to RPC input args struct
    void* outputs;     // pointer to RPC output args struct
    size_t inputs_sz;  // if non-zero, we are allocating space for input_args
    size_t outputs_sz; // if non-zero, we are allocating space for output_args
    
    void* bulk_buf;
    size_t bulk_sz;
    hg_bulk_t bulk;    // set this to free bulk on cleanup
    
    int initiator;     // set to 1 when this process initiated rpc
    int have_input;    // set to 1 when margo_get_input() is successful
    int have_output;   // set to 1 when margo_get_output() is successful
} rpc_state;

// ========== Mercury RPC serialization of custom structs ==========

/* encode/decode struct timespec */
typedef struct timespec sys_timespec_t;
MERCURY_GEN_STRUCT_PROC(sys_timespec_t,
    ((uint64_t)(tv_sec))
    ((uint64_t)(tv_nsec))
)

/* encode/decode unifyfs_file_attr_t */
MERCURY_GEN_STRUCT_PROC(unifyfs_file_attr_t,
    ((int32_t)(gfid))
    ((int32_t)(is_laminated))
    ((int32_t)(is_shared))
    ((uint32_t)(mode))
    ((uint32_t)(uid))
    ((uint32_t)(gid))
    ((hg_size_t)(size))
    ((sys_timespec_t)(atime))
    ((sys_timespec_t)(ctime))
    ((sys_timespec_t)(mtime))
    ((hg_const_string_t)(filename))
)

/* encode/decode unifyfs_extent_t */
MERCURY_GEN_STRUCT_PROC(unifyfs_extent_t,
    ((hg_size_t)(offset))
    ((hg_size_t)(length))
    ((int32_t)(gfid)))

    /* encode/decode unifyfs_extent_t */
MERCURY_GEN_STRUCT_PROC(unifyfs_data_chunk_t,
    ((hg_size_t)(file_offset))
    ((hg_size_t)(log_offset))
    ((hg_size_t)(length))
    ((int32_t)(gfid))
    ((int32_t)(log_app_id))
    ((int32_t)(log_client_id))
    ((int32_t)(log_server)))

#endif /* __UNIFYFS_RPC_TYPES_H */
