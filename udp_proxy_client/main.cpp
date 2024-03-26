#include <iostream>
#include <uv.h>
#include <unordered_set>
#include <chrono>
#include <list>
#include <csignal>
#include <unistd.h>
#include <cstring>
#include <getopt.h>

using namespace std;

#define UDP_RECV_BUFFER_LEN 2048
uint8_t udpRecvBuf[UDP_RECV_BUFFER_LEN];

const static int TCP_RECV_BUFFER_LEN = 1024 * 1024 * 1; // 1MB
uint8_t tcpRecvBuf[TCP_RECV_BUFFER_LEN];

uv_loop_t *loop = nullptr;
int64_t now_ms = 0;
bool g_debug = false;

std::string udp_server_ip = "127.0.0.1";
int udp_server_port = 1106;

std::string dest_udp_server_ip = "127.0.0.1";
int dest_udp_server_port = 58033;

std::string tcp_server_ip = "127.0.0.1";
int out_tcp_server_port = 1107;

std::string dest_tcp_server_ip = "127.0.0.1";
int dest_tcp_server_port = 1107;

int64_t time_out_ms = 3000;
const int heart_bit_buf_size = 2;
uint8_t heart_bit_buf[heart_bit_buf_size];

bool g_enable_udp = true;
int  g_spare_count = 5;

struct NetBuffer {
    uint8_t * data{};
    int data_length = 0;
};

struct ConnectInfo {
    // udp
    uv_udp_t udp_watcher_to_dest{};
    uv_udp_t udp_watcher_to_out{};
    int64_t last_recv_time = 0;
    bool in_temporarily_unused_set = true;
    int recv_bytes = 0;
    int send_bytes = 0;

    // tcp
    uv_tcp_t tcp_watcher_to_server{};
    uv_connect_t connect_req_to_server{};
    uv_write_t tcp_write_req_to_server{};
    std::list<NetBuffer> send_queue_to_server;
    bool has_closed_to_server = false;
    bool server_has_connected = false;
    bool server_is_sending = false;

    uv_tcp_t tcp_watcher_to_dest{};
    uv_connect_t connect_req_to_dest{};
    uv_write_t tcp_write_req_to_dest{};
    std::list<NetBuffer> send_queue_to_dest;
    bool has_closed_to_dest = false;
    bool local_has_connected = false;
    bool local_is_sending = false;

    bool need_close_to_server = false;
    bool need_close_to_local = false;

    bool to_server_closed = false;
    bool to_local_closed = false;

    void reset() {
        recv_bytes = 0;
        send_bytes = 0;
    }
};

// 没人使用的连接
std::unordered_set<ConnectInfo*> temporarily_unused_udp_set;
// 正在使用的连接
std::unordered_set<ConnectInfo*> using_set;
std::unordered_set<ConnectInfo*> temporarily_unused_tcp_set;

void session_enable_write(ConnectInfo *session, bool to_server);
bool tcp_connect_to_dest(ConnectInfo* info);
bool tcp_connect_to_out(ConnectInfo* info);
void close_connect_info(ConnectInfo* pConnectInfo, bool is_to_server);
int64_t getSysTimeMs();
void check_and_add_tcp();

void alloc_cb_udp(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
{
    buf->base = (char *) udpRecvBuf;
    buf->len = UDP_RECV_BUFFER_LEN;
}

void alloc_cb_tcp(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    buf->base = (char *) tcpRecvBuf;
    buf->len = TCP_RECV_BUFFER_LEN;
}

void udp_cb(uv_udp_t *handle,
            ssize_t size,
            const uv_buf_t *buf,
            const struct sockaddr *addr,
            unsigned flags) {
    if (size <= 0) return;
    auto* info = (ConnectInfo*)handle->data;
    if (info->in_temporarily_unused_set) {
        info->in_temporarily_unused_set = false;
        temporarily_unused_udp_set.erase(info);
        using_set.insert(info);
    }
    info->last_recv_time = now_ms;
    uv_buf_t bufs[1];
    bufs[0].base = buf->base;
    bufs[0].len = size;

    if (handle == &info->udp_watcher_to_dest) {
        info->send_bytes += size;
        uv_udp_try_send(&info->udp_watcher_to_out, bufs,1, nullptr);
    } else {
        info->recv_bytes += size;
        uv_udp_try_send(&info->udp_watcher_to_dest, bufs, 1, nullptr);
    }
}

void write_cb(uv_write_t* req, int status)
{
    auto * session = (ConnectInfo*)req->data;
    bool is_server = (req == &session->tcp_write_req_to_server);
    if (status != 0) {
        if (g_debug) {
            printf("is_server:%d write error %d\n",is_server,status);
        }
        return;
    }

    if (is_server) {
        delete[] session->send_queue_to_server.front().data;
        session->send_queue_to_server.pop_front();
        session->server_is_sending = false;
    } else {
        delete[] session->send_queue_to_dest.front().data;
        session->send_queue_to_dest.pop_front();
        session->local_is_sending = false;
    }
    session_enable_write(session, is_server);
}

void session_enable_write(ConnectInfo *session, bool to_server) {
    if (to_server) {
        if (!session->server_has_connected || session->has_closed_to_server || session->send_queue_to_server.empty()
            || !uv_is_active(reinterpret_cast<uv_handle_t *>(&session->tcp_watcher_to_server))) {
            if (session->need_close_to_server) {
                close_connect_info(session, to_server);
            }
            return;
        }
        if (session->server_is_sending ) {
            return;
        }
        session->server_is_sending = true;
        uv_buf_t bufs[1];
        bufs[0].base = reinterpret_cast<char *>(session->send_queue_to_server.front().data);
        bufs[0].len = session->send_queue_to_server.front().data_length;
        uv_write(&session->tcp_write_req_to_server, (uv_stream_t*)&session->tcp_watcher_to_server, bufs, 1, write_cb);
    } else {
        if (!session->local_has_connected || session->has_closed_to_dest || session->send_queue_to_dest.empty()
           || !uv_is_active(reinterpret_cast<uv_handle_t *>(&session->tcp_watcher_to_dest))) {
            if (session->need_close_to_local) {
                close_connect_info(session, to_server);
            }
            return;
        }
        if (session->local_is_sending) {
            return;
        }
        session->local_is_sending = true;
        uv_buf_t bufs[1];
        bufs[0].base = reinterpret_cast<char *>(session->send_queue_to_dest.front().data);
        bufs[0].len = session->send_queue_to_dest.front().data_length;
        uv_write(&session->tcp_write_req_to_dest, (uv_stream_t*)&session->tcp_watcher_to_dest, bufs, 1, write_cb);
    }
}

void delete_connect_info(ConnectInfo* connect) {
    temporarily_unused_tcp_set.erase(connect);
    delete connect;
}

void close_connect_info(ConnectInfo* pConnectInfo, bool is_to_server) {
    if (is_to_server) {
        if (pConnectInfo->to_server_closed) {
            return;
        }
        pConnectInfo->to_server_closed = true;
        pConnectInfo->to_local_closed = true;
        uv_close(reinterpret_cast<uv_handle_t *>(&pConnectInfo->tcp_watcher_to_server), [](uv_handle_t *handle) {
            auto* pSession = (ConnectInfo*)handle->data;
            if (pSession->local_has_connected && uv_is_active(reinterpret_cast<uv_handle_t *>(&pSession->tcp_watcher_to_dest))) {
                if (!pSession->local_is_sending && pSession->send_queue_to_dest.empty()) {
                    uv_close(reinterpret_cast<uv_handle_t *>(&pSession->tcp_watcher_to_dest), [](uv_handle_t *handle) {
                        auto* pSession = (ConnectInfo*)handle->data;
                        delete_connect_info( pSession);
                    });
                } else {
                    pSession->to_local_closed = false;
                    pSession->need_close_to_local = true;
                }
            } else {
                delete_connect_info( pSession);
            }
        });
    } else {
        if (pConnectInfo->to_local_closed) {
            return;
        }
        pConnectInfo->to_server_closed = true;
        pConnectInfo->to_local_closed = true;
        uv_close(reinterpret_cast<uv_handle_t *>(&pConnectInfo->tcp_watcher_to_dest), [](uv_handle_t *handle) {
            auto* pSession = (ConnectInfo*)handle->data;
            if (uv_is_active(reinterpret_cast<uv_handle_t *>(&pSession->tcp_watcher_to_server))) {
                if(!pSession->server_is_sending && pSession->send_queue_to_server.empty()) {
                    uv_close(reinterpret_cast<uv_handle_t *>(&pSession->tcp_watcher_to_server), [](uv_handle_t *handle) {
                        auto* pSession = (ConnectInfo*)handle->data;
                        delete_connect_info( pSession);
                    });
                } else {
                    pSession->to_server_closed = false;
                    pSession->need_close_to_server = true;
                }
            } else {
                delete_connect_info( pSession);
            }
        });
    }
}

void tcp_cb(uv_stream_t *handle, ssize_t nRead, const uv_buf_t *buf) {
    auto * pConnectInfo = (ConnectInfo *) handle->data;
    auto *sourceData = (uint8_t *) buf->base;
    int sourceDataLen = nRead;

    bool is_to_server = handle == pConnectInfo->connect_req_to_server.handle;

    if (nRead <= 0) {
        close_connect_info(pConnectInfo, is_to_server);
        return;
    }
    check_and_add_tcp();

    NetBuffer buffer;
    buffer.data_length = sourceDataLen;
    buffer.data = new uint8_t [buffer.data_length];
    memcpy(buffer.data, sourceData, buffer.data_length);

    if (is_to_server) {
        pConnectInfo->send_queue_to_dest.push_back(buffer);
        if (!pConnectInfo->local_has_connected) {
            if (!tcp_connect_to_dest(pConnectInfo)) {
                close_connect_info(pConnectInfo, is_to_server);
            }
            temporarily_unused_tcp_set.erase(pConnectInfo);
        }
    } else {
        pConnectInfo->send_queue_to_server.push_back(buffer);
    }
    session_enable_write(pConnectInfo ,!is_to_server);
}

void conn_connect_done(uv_connect_t *req, int status) {
    auto *pConnectInfo = (ConnectInfo *) req->handle->data;
    bool is_to_server = (req == &pConnectInfo->connect_req_to_server);
    if (status != 0) {
        close_connect_info(pConnectInfo, is_to_server);
        return;  /* Handle has been closed. */
    }
    pConnectInfo->server_has_connected = true;
    uv_read_start(req->handle, alloc_cb_tcp, tcp_cb);
    session_enable_write(pConnectInfo, is_to_server);
}

bool tcp_connect_to_out(ConnectInfo* info)
{
    struct sockaddr_in sin{};
    int ret;
    uv_ip4_addr(tcp_server_ip.c_str(), out_tcp_server_port, &sin);
    info->tcp_watcher_to_server.data = info;
    info->connect_req_to_server.data = info;
    info->tcp_write_req_to_server.data = info;

    uv_tcp_init(loop, &info->tcp_watcher_to_server);
    uv_tcp_keepalive(&info->tcp_watcher_to_server, 1, 3);
    uv_tcp_nodelay(&info->tcp_watcher_to_server, 1);
    ret = uv_tcp_connect(&info->connect_req_to_server, &info->tcp_watcher_to_server, (const struct sockaddr *) &sin, conn_connect_done);

    if (ret != 0) {
        return false;
    }

    return ret == 0;
}

bool tcp_connect_to_dest(ConnectInfo* info)
{
    if (info->local_has_connected) {
        return true;
    }
    struct sockaddr_in sin{};
    int ret;
    info->local_has_connected = true;
    uv_ip4_addr(dest_tcp_server_ip.c_str(), dest_tcp_server_port, &sin);
    info->tcp_watcher_to_dest.data = info;
    info->connect_req_to_dest.data = info;
    info->tcp_write_req_to_dest.data = info;

    uv_tcp_init(loop, &info->tcp_watcher_to_dest);
    uv_tcp_keepalive(&info->tcp_watcher_to_dest, 1, 3);
    uv_tcp_nodelay(&info->tcp_watcher_to_dest, 1);
    ret = uv_tcp_connect(&info->connect_req_to_dest, &info->tcp_watcher_to_dest, (const struct sockaddr *) &sin, conn_connect_done);

    if (ret != 0) {
        return false;
    }
    return true;
}

bool add_temporarily_unused_connect() {
    struct sockaddr_in sin_addr{};
    auto* info = new ConnectInfo;
    int ret;
    {
        uv_ip4_addr(udp_server_ip.c_str(), udp_server_port, &sin_addr);
        do {
            info->udp_watcher_to_out.data = info;
            ret = uv_udp_init(loop, &info->udp_watcher_to_out);
            if (ret != 0) break;
            ret = uv_udp_connect(&info->udp_watcher_to_out, (const struct sockaddr *)&sin_addr);
            if (ret != 0) break;
            ret = uv_udp_recv_start(&info->udp_watcher_to_out, alloc_cb_udp, udp_cb);
            if (ret != 0) break;
        } while (false);

        if (ret != 0) {
            uv_close(reinterpret_cast<uv_handle_t *>(&info->udp_watcher_to_out), [](uv_handle_t *handle) {
                auto info = (ConnectInfo*)handle->data;
                delete info;
            });
            return false;
        }
    }
    {
        uv_ip4_addr(dest_udp_server_ip.c_str(), dest_udp_server_port, &sin_addr);
        do {
            info->udp_watcher_to_dest.data = info;
            ret = uv_udp_init(loop, &info->udp_watcher_to_dest);
            if (ret != 0) break;
            ret = uv_udp_connect(&info->udp_watcher_to_dest, (const struct sockaddr *)&sin_addr);
            if (ret != 0) break;
            ret = uv_udp_recv_start(&info->udp_watcher_to_dest, alloc_cb_udp, udp_cb);
            if (ret != 0) break;
        } while (false);
        if (ret != 0) {
            uv_close(reinterpret_cast<uv_handle_t *>(&info->udp_watcher_to_out), [](uv_handle_t *handle) {
                auto info = (ConnectInfo*)handle->data;
                uv_close(reinterpret_cast<uv_handle_t *>(&info->udp_watcher_to_dest), [](uv_handle_t *handle) {
                    auto info = (ConnectInfo*)handle->data;
                    delete info;
                });

            });
            return false;
        }
    }
    temporarily_unused_udp_set.insert(info);
    return true;
}

static void fun_log_timer(uv_timer_t* handle)
{
    for (auto v : using_set) {
        printf("%p recv: %d send: %d\n",v, v->recv_bytes,v->send_bytes);
    }
}

void check_and_add_tcp() {
    while (temporarily_unused_tcp_set.size() < g_spare_count) {
        auto* info = new ConnectInfo;
        if (!tcp_connect_to_out(info)) {
            close_connect_info(info, true);
            return;
        }
        NetBuffer buffer;
        buffer.data_length = heart_bit_buf_size;
        buffer.data = new uint8_t[buffer.data_length];
        memcpy(buffer.data, heart_bit_buf, heart_bit_buf_size);
        info->send_queue_to_server.push_back(buffer);
        temporarily_unused_tcp_set.insert(info);
    }
}

static void fun_check_timer(uv_timer_t* handle)
{
    if (g_enable_udp) {
        while (temporarily_unused_udp_set.size() < g_spare_count) {
            if (!add_temporarily_unused_connect()) {
                break;
            }
        }

        for (auto&v: temporarily_unused_udp_set) {
            uv_buf_t bufs[1];
            bufs[0].base = reinterpret_cast<char *>(heart_bit_buf);
            bufs[0].len = heart_bit_buf_size;
            uv_udp_try_send(&v->udp_watcher_to_out, bufs, 1, nullptr);
        }

        now_ms = getSysTimeMs();
        for (auto v = using_set.begin(); v != using_set.end();) {
            if (now_ms - (*v)->last_recv_time > time_out_ms) {
                (*v)->in_temporarily_unused_set = true;
                (*v)->reset();
                temporarily_unused_udp_set.insert(*v);
                v = using_set.erase(v);
            } else {
                v++;
            }
        }
    }

    check_and_add_tcp();
}

int InitDaemon() {
    if (fork() != 0) {
        exit(0);
    }
    setsid();
    signal(SIGHUP, SIG_IGN);
    if (fork() != 0) {
        exit(0);
    }
    return 0;
}

static void signal_pipe_fun(int signal_type) {
}

void print_help() {
    fprintf(stderr, "Usage: out_server [OPTION]\n"
                    "\t-i, --udp_server_ip\n "
                    "\t-p, --udp_server_port\n "
                    "\t-I, --dest_udp_server_ip\n "
                    "\t-P, --dest_udp_server_port\n "
                    "\t-x, --disable_udp\n"
                    "\t-m, --tcp_server_ip\n "
                    "\t-n, --out_tcp_server_port\n "
                    "\t-M, --dest_tcp_server_ip\n "
                    "\t-N, --dest_tcp_server_port\n "
                    "\t-d, --daemon\n"
                    "\t-D, --debug\n"
    );
}

int main(int argc, char *argv[]) {
    int c;  //输入标记
    int help_flag = 0;
    bool is_daemon = false;
    struct option longOpts[] =
            {
                    {"udp_server_ip",     required_argument, 0,        'i'},
                    {"udp_server_port",   required_argument, 0,        'p'},
                    {"udp_dest_ip",       required_argument, 0,        'I'},
                    {"udp_dest_port",     required_argument, 0,        'P'},
                    {"disable_udp",       no_argument,       0,        'x'},
                    {"tcp_server_ip",     required_argument, 0,        'm'},
                    {"tcp_server_port",   required_argument, 0,        'n'},
                    {"tcp_dest_ip",       required_argument, 0,        'M'},
                    {"tcp_dest_port",     required_argument, 0,        'N'},
                    {"g_spare_count",     required_argument, 0,        'c'},
                    {"daemon",            no_argument,       0,        'd'},
                    {"debug",             no_argument,       0,        'D'},
                    {"help",             0,                 &help_flag, 1},
                    {0,                  0,                 0,          0}
            };

    //有：表示有参数，两个：表示参数可选
    while ((c = getopt_long(argc, argv, "i:p:I:xP:dDhc:m:n:M:N:?", longOpts, nullptr)) != EOF) {
        switch (c) {
            case 'i':
                udp_server_ip = optarg;
                break;
            case 'p':
                udp_server_port = atoi(optarg);
                break;
            case 'm':
                tcp_server_ip = optarg;
                break;
            case 'n':
                out_tcp_server_port = atoi(optarg);
                break;
            case 'M':
                dest_tcp_server_ip = optarg;
                break;
            case 'N':
                dest_tcp_server_port = atoi(optarg);
                break;
            case 'I':
                dest_udp_server_ip = optarg;
                break;
            case 'P':
                dest_udp_server_port = atoi(optarg);
                break;
            case 'c':
                g_spare_count = atoi(optarg);
            case 'd':
                is_daemon = true;
                break;
            case 'D':
                g_debug = true;
                break;
            case 'h':
                help_flag = true;
                break;
            case 'x':
                g_enable_udp = false;
                break;
            case '?':
                print_help();
                exit(0);
            default:
                break;
        }
    }
    if (help_flag) {
        print_help();
        exit(0);
    }

    printf("out server: udp-serve:%s:%d, udp-dest:%s:%d  tcp-server:%s:%d tcp-dest:%s:%d\n",
           udp_server_ip.c_str(), udp_server_port, dest_udp_server_ip.c_str(), dest_udp_server_port,
           tcp_server_ip.c_str(), out_tcp_server_port, dest_tcp_server_ip.c_str(), dest_tcp_server_port);

    if (is_daemon) {
        InitDaemon();
    }

    signal(SIGPIPE, signal_pipe_fun);

    loop = uv_loop_new();
    uv_loop_init(loop);

    heart_bit_buf[0] = 'O';
    heart_bit_buf[1] = 'K';

    uv_timer_t check_timer;
    uv_timer_init(loop, &check_timer);
    uv_timer_start(&check_timer, fun_check_timer,0, 10);

    uv_timer_t log_timer;
    uv_timer_init(loop, &log_timer);
    uv_timer_start(&log_timer, fun_log_timer,1000, 1000);

    uv_run(loop,UV_RUN_DEFAULT);
    return 0;
}

int64_t getSysTimeMs() {
    chrono::time_point<chrono::system_clock, chrono::milliseconds> tpMicro
            = chrono::time_point_cast<chrono::milliseconds>(chrono::system_clock::now());
    return tpMicro.time_since_epoch().count();;
}
