/*
 * C ABI for the deepmsg cluster client (C3b).
 *
 * Shape modelled on `aeron_archive_*`: an async connect handle that is polled,
 * a blocking wrapper over it, and a client handle whose egress is polled with a
 * plain C callback. The C++ session lives behind the opaque types.
 *
 * Payload semantics are the reference's: an offered buffer carries only the
 * application bytes, the 32-byte session header is written by the client.
 */
#ifndef DEEPMSG_CLUSTER_CLIENT_H
#define DEEPMSG_CLUSTER_CLIENT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct deepmsg_cluster_client_stct deepmsg_cluster_client_t;
    typedef struct deepmsg_cluster_async_connect_stct deepmsg_cluster_async_connect_t;

    /**
     * Called for each egress session message of this client's session.
     *
     * The buffer points into the polled term and is valid only for the call.
     */
    typedef void (*deepmsg_cluster_message_handler_t)(void* clientd, int64_t cluster_session_id, int64_t timestamp,
                                                      const uint8_t* buffer, size_t length);

    /**
     * Start an asynchronous connect. `aeron_dir` may be NULL for the default; the
     * two channels are required (`aeron:ipc` for a same-driver cluster).
     *
     * Returns 0 on success, -1 on an immediately refused configuration.
     */
    int deepmsg_cluster_async_connect(deepmsg_cluster_async_connect_t** async, const char* aeron_dir,
                                      const char* ingress_channel, const char* egress_channel);

    /**
     * Advance the connect: 0 while pending, 1 when `*client` is set, -1 on error
     * (`deepmsg_cluster_error()` has the reason). The handle is freed on success
     * and on error.
     */
    int deepmsg_cluster_async_connect_poll(deepmsg_cluster_client_t** client, deepmsg_cluster_async_connect_t* async);

    /**
     * Free a connect handle that was abandoned (not needed after a 1 or -1 poll).
     */
    void deepmsg_cluster_async_connect_free(deepmsg_cluster_async_connect_t* async);

    /**
     * Blocking connect: `async_connect` + poll until done, driving the client.
     * Returns 0 on success, -1 on error.
     */
    int deepmsg_cluster_connect(deepmsg_cluster_client_t** client, const char* aeron_dir, const char* ingress_channel,
                                const char* egress_channel);

    /**
     * Offer one application message. Returns the publication position, or a
     * negative publication result (`AERON_PUBLICATION_*`).
     */
    int64_t deepmsg_cluster_offer(deepmsg_cluster_client_t* client, const uint8_t* buffer, size_t length);

    /**
     * Poll the egress subscription and dispatch session messages to `handler`.
     * Returns the number of fragments consumed, or -1 on error.
     */
    int deepmsg_cluster_poll_egress(deepmsg_cluster_client_t* client, deepmsg_cluster_message_handler_t handler,
                                    void* clientd, size_t fragment_limit);

    /** The session id, or -1 before the connect completes. */
    int64_t deepmsg_cluster_session_id(const deepmsg_cluster_client_t* client);

    /** 1 when the session is closed (locally or by the cluster), 0 otherwise. */
    int deepmsg_cluster_is_closed(const deepmsg_cluster_client_t* client);

    /**
     * Close the session (sending `SessionCloseRequest` when connected) and release
     * the handle. Returns 0, or -1 if `client` was NULL.
     */
    int deepmsg_cluster_close(deepmsg_cluster_client_t* client);

    /** The last error text of the calling thread, never NULL. */
    const char* deepmsg_cluster_error(void);

#ifdef __cplusplus
}
#endif

#endif /* DEEPMSG_CLUSTER_CLIENT_H */
