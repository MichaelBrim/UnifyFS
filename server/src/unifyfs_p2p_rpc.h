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

#ifndef _UNIFYFS_P2P_RPC_H
#define _UNIFYFS_P2P_RPC_H

#include "unifyfs_global.h"
#include "extent_tree.h"
#include "margo_server.h"
#include "unifyfs_inode.h"
#include "unifyfs_request_manager.h"
#include "unifyfs_service_manager.h"


/* determine server responsible for maintaining target file's metadata */
int hash_gfid_to_server(int gfid);

/* server peer-to-peer (p2p) margo request structure */
typedef struct {
    rpc_state* req_state;
    hg_addr_t peer;
    int peer_rank;
    int gfid;             // target of remote request
    server_rpc_e req_op;  // operation on target

    /* client reqs dependent on this remote req (if any )*/
    client_rpc_req_t* client_req;     // for only one
    arraylist_t* pending_client_reqs; // for more than one

    ABT_cond pending_cond;  /* condition to signal upon pending completion */
    ABT_mutex pending_sync; /* mutex for above condition variable */
    int pending_waiters;    /* track number of pending waiters */
} p2p_request;

/* helper method to initialize peer rpc request */
int init_p2p_request(server_rpc_e request_op,
                     int peer_rank,
                     int gfid,
                     p2p_request* req);

/* helper method to forward peer rpc request */
int forward_p2p_request(p2p_request* req);

/* helper method to wait for peer rpc request completion */
int wait_for_p2p_request(p2p_request* req);

/* helper method to cleanup peer rpc request state */
void cleanup_p2p_request(p2p_request* preq);

/* methods for pending remote request bookkeeping */
bool check_pending_remote_request(int peer_rank,
                                  int gfid,
                                  server_rpc_e op,
                                  p2p_request** preqp);
int add_pending_remote_request(int peer_rank,
                               int gfid,
                               server_rpc_e op,
                               client_rpc_req_t* client_req,
                               p2p_request** preqp);
int clear_pending_remote_request(p2p_request* preq);

/*** Point-to-point Server RPCs ***/

/**
 * @brief Read a chunk from remote server
 *
 * @param chunk        remote chunk data location descriptor
 * @param bulk_offset  offset with bulk_extent to write data
 * @param bulk_extent  bulk representing local buffer
 *
 * @param[out] bytes_read  number of chunk bytes read
 *
 * @return success|failure
 */
int unifyfs_invoke_read_chunk_rpc(unifyfs_data_chunk_t* chunk,
                                  size_t bulk_offset,
                                  hg_bulk_t bulk_extent,
                                  size_t* bytes_read);

/**
 * @brief Request chunk reads from remote server
 *
 * @param dst_srvr_rank  remote server rank
 * @param rdreq          read request structure
 * @param remote_reads   server chunk reads
 *
 * @return success|failure
 */
int invoke_chunk_read_request_rpc(int dst_srvr_rank,
                                  server_read_req_t* rdreq,
                                  server_chunk_reads_t* remote_reads);
/**
 * @brief Respond to chunk read request
 *
 * @param scr  server chunk reads structure
 *
 * @return success|failure
 */
int invoke_chunk_read_response_rpc(server_chunk_reads_t* scr);

/**
 * @brief Add new extents to target file
 *
 * @param gfid         target file
 * @param num_extents  length of file extents array
 * @param extents      array of extents to add
 *
 * @return success|failure
 */
int unifyfs_invoke_add_extents_rpc(int gfid,
                                   unsigned int num_extents,
                                   extent_metadata* extents);


/* Lookup extent locations for target file */
int unifyfs_find_extent_chunks(unifyfs_fops_ctx_t* ctx,
                               int gfid,
                               unsigned int num_extents,
                               unifyfs_extent_t* extents,
                               unsigned int* num_chunks,
                               unifyfs_data_chunk_t** chunks);

/**
 * @brief Find location of extents for target file
 *
 * @param gfid  target file
 *
 * @param[in,out] timestamp  extents cache timestamp (in: local, out: owner)
 *
 * @return success|failure
 */
int unifyfs_invoke_get_extents_rpc(int gfid,
                                   struct timespec* timestamp);

/**
 * @brief Laminate the target file
 *
 * @param gfid  target file
 *
 * @return success|failure
 */
int unifyfs_invoke_laminate_rpc(unifyfs_fops_ctx_t* ctx,
                                int gfid);

/**
 * @brief Get metadata for target file
 *
 * @param gfid    target file
 * @param create  flag indicating if this is a newly created file
 * @param attrs   file attributes to update
 *
 * @return success|failure
 */
int unifyfs_invoke_metaget_rpc(unifyfs_fops_ctx_t* ctx,
                               int gfid,
                               unifyfs_file_attr_t* attrs);

/**
 * @brief Update metadata for target file
 *
 * @param gfid     target file
 * @param attr_op  metadata operation that triggered update
 * @param attrs    file attributes to update
 *
 * @return success|failure
 */
int unifyfs_invoke_metaset_rpc(unifyfs_fops_ctx_t* ctx,
                               int gfid, int attr_op,
                               unifyfs_file_attr_t* attrs);

/**
 * @brief Transfer target file
 *
 * @param client_app      requesting client app id
 * @param client_id       requesting client id
 * @param transfer_id     requesting client transfer id
 * @param gfid            target file
 * @param transfer_mode   transfer mode
 * @param dest_file       destination file
 *
 * @return success|failure
 */
int unifyfs_invoke_transfer_rpc(int client_app,
                                int client_id,
                                int transfer_id,
                                int gfid,
                                int transfer_mode,
                                const char* dest_file);

/**
 * @brief Truncate target file
 *
 * @param gfid      target file
 * @param filesize  truncated file size
 *
 * @return success|failure
 */
int unifyfs_invoke_truncate_rpc(unifyfs_fops_ctx_t* ctx,
                                int gfid, size_t filesize);

/**
 * @brief Report pid of local server to rank 0 server
 *
 * @return success|failure
 */
int unifyfs_invoke_server_pid_rpc(void);

#endif // UNIFYFS_P2P_RPC_H
