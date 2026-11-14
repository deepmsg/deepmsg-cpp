/*
 * The shim's signatures, and nothing else.
 *
 * Transcribed from the deepmsg Rust track's `crates/dpdk/src/sys.rs`, which
 * mirrors `shim/shim.c` field for field. No DPDK type appears here: a handle
 * into DPDK is `void *`, the structs are this file's own, and no DPDK
 * structure's layout reaches the C++ side.
 */

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /* One frame received: the handle, its bytes, and the NIC's verdict. */
    typedef struct
    {
        void* mbuf;
        unsigned char* data;
        uint16_t len;
        uint32_t checksum;
    } shim_rx_desc;

#define SHIM_CKSUM_NONE 0u
#define SHIM_CKSUM_GOOD 1u
#define SHIM_CKSUM_BAD 2u

    /*
     * One frame to transmit: allocated by the shim, written here, handed back in a
     * burst. `ol_flags` is the offload the caller wants for this frame.
     */
    typedef struct
    {
        void* mbuf;
        unsigned char* data;
        uint16_t room;
        uint16_t len;
        uint64_t ol_flags;
        uint16_t l2_len;
        uint16_t l3_len;
    } shim_tx_slot;

    /* What a port is, as far as this build asks. */
    typedef struct
    {
        char driver[64];
        uint16_t max_rx_queues;
        uint16_t max_tx_queues;
        uint16_t max_mtu;
        uint16_t min_mtu;
        int socket_id;
        uint64_t rx_offloads;
        uint64_t tx_offloads;
    } shim_port_info_t;

    /* What a port's own counters are, as this build reads them. */
    typedef struct
    {
        uint64_t ipackets;
        uint64_t opackets;
        uint64_t ibytes;
        uint64_t obytes;
        uint64_t imissed;
        uint64_t ierrors;
        uint64_t oerrors;
        uint64_t rx_nombuf;
    } shim_port_stats_t;

    /* The link's state, with DPDK's bitfields already resolved. */
    typedef struct
    {
        uint32_t speed_mbps;
        uint8_t up;
        uint8_t full_duplex;
    } shim_port_link_t;

#define SHIM_XSTAT_NAME_MAX 64

    typedef struct
    {
        char name[SHIM_XSTAT_NAME_MAX];
    } shim_xstat_name_t;

/* One burst carries at most this many frames. */
#define SHIM_BURST_MAX 64

/*
 * The fewest frames a receive path can be asked for and still answer with any:
 * ixgbe's vector receive path floor-aligns the count, and answers nothing
 * below four.
 */
#define SHIM_BURST_MIN 4

    /* ---- the EAL, once per process ---------------------------------------- */

    int shim_eal_init(int argc, char** argv);
    int shim_eal_cleanup(void);
    int shim_rte_errno(void);
    const char* shim_rte_strerror(int err);

    int shim_save_affinity(void);
    int shim_restore_affinity(void);

    int shim_thread_register(void);
    void shim_thread_unregister(void);
    unsigned shim_lcore_id(void);
    unsigned shim_lcore_count(void);

    /* ---- the pool the frames come from ------------------------------------ */

    void* shim_pool_create(const char* name, unsigned count, unsigned cache, unsigned data_room, int socket_id);
    void shim_pool_free(void* pool);
    unsigned shim_pool_avail(void* pool);

    /* ---- the port, and its queues ----------------------------------------- */

    unsigned shim_port_count(void);
    int shim_port_by_name(const char* name);
    void shim_port_name(unsigned port, char* out, unsigned cap);
    int shim_port_info(unsigned port, shim_port_info_t* out);

    uint64_t shim_rx_offload_ipv4_checksum(void);
    uint64_t shim_rx_offload_udp_checksum(void);
    uint64_t shim_tx_offload_ipv4_checksum(void);
    uint64_t shim_tx_offload_udp_checksum(void);
    uint64_t shim_mbuf_tx_ipv4(void);
    uint64_t shim_mbuf_tx_ip_checksum(void);
    uint64_t shim_mbuf_tx_udp_checksum(void);

    int shim_port_configure(unsigned port, unsigned nb_rx, unsigned nb_tx, uint64_t rx_offloads, uint64_t tx_offloads);
    int shim_rx_queue_setup(unsigned port, unsigned queue, unsigned descriptors, void* pool);
    int shim_tx_queue_setup(unsigned port, unsigned queue, unsigned descriptors);
    int shim_port_start(unsigned port);
    void shim_port_stop(unsigned port);
    void shim_port_close(unsigned port);
    int shim_port_mac(unsigned port, unsigned char* out);
    int shim_port_mtu(unsigned port, uint16_t* out);
    int shim_port_stats(unsigned port, shim_port_stats_t* out);
    int shim_port_link(unsigned port, shim_port_link_t* out);
    int shim_port_xstats_count(unsigned port);
    int shim_port_xstats_names(unsigned port, shim_xstat_name_t* out, unsigned n);
    int shim_port_xstats_values(unsigned port, uint64_t* out, unsigned n);

    /* ---- the data path, in batches ---------------------------------------- */

    unsigned shim_rx_burst(unsigned port, unsigned queue, shim_rx_desc* out, unsigned max);
    unsigned shim_tx_alloc(void* pool, shim_tx_slot* out, unsigned count);
    unsigned shim_tx_burst(unsigned port, unsigned queue, shim_tx_slot* slots, unsigned count);
    void shim_mbuf_info_bulk(void* const* mbufs, shim_rx_desc* out, unsigned count);
    void shim_mbuf_free_bulk(void** mbufs, unsigned count);

    /* ---- a ring, for the frames that cross threads ------------------------ */

    void* shim_ring_create(const char* name, unsigned capacity);
    void shim_ring_free(void* ring);
    unsigned shim_ring_enqueue_burst(void* ring, void* const* mbufs, unsigned count);
    unsigned shim_ring_dequeue_burst(void* ring, void** mbufs, unsigned count);

#ifdef __cplusplus
}
#endif
