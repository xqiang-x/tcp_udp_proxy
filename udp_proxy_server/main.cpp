#include <iostream>
#include <uv.h>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <chrono>
#include <unistd.h>
#include <getopt.h>
#include <cstring>
#include <sys/wait.h>

using namespace std;

#define UDP_KEY(s_port, s_ip) (((uint64_t)s_port<<16)|(uint64_t)0|((uint64_t)s_ip<<32))

#define UDP_RECV_BUFFER_LEN 2048

struct ConnectInfo {
    struct sockaddr_in dest_sin_addr{};
    struct sockaddr_in client_sin_addr{};
    int64_t last_recv_time = 0;
    uint64_t rts_key{};
    uint64_t client_key{};
    int recv_byte = 0;
    int send_byte = 0;
};

struct NetBuffer {
    uint8_t *data{};
    int data_length = 0;
};

struct TcpConnectInfo {
    ~TcpConnectInfo() {
        if (unidentified) {
            free(unidentified);
        }
        if (tcp_watcher_to_server) {
            free(tcp_watcher_to_server);
        }
        if (tcp_watcher_to_dest) {
            free(tcp_watcher_to_dest);
        }
        while (!send_queue_to_server.empty()) {
            delete []send_queue_to_server.front().data;
            send_queue_to_server.pop_front();
        }

        while (!send_queue_to_dest.empty()) {
            delete []send_queue_to_dest.front().data;
            send_queue_to_dest.pop_front();
        }
    }

    uv_tcp_t *unidentified = nullptr;
    uv_tcp_t *tcp_watcher_to_server = nullptr;
    uv_tcp_t *tcp_watcher_to_dest = nullptr;

    uv_write_t tcp_write_req_to_server{};
    std::list<NetBuffer> send_queue_to_server;
    bool server_is_sending = false;
    bool server_close = false;
    bool server_closed = false;

    uv_write_t tcp_write_req_to_dest{};
    std::list<NetBuffer> send_queue_to_dest;
    bool local_is_sending = false;
    bool local_close = false;
    bool local_closed = false;
};

struct PreparationInfo {
    PreparationInfo(struct sockaddr_in a, int64_t now) : udp_addr(a), last_recv_time(now) {};
    struct sockaddr_in udp_addr;
    int64_t last_recv_time = 0;
};

struct OutOfOrderBuffer {
    uint8_t buffer[2000]{};
    uint32_t buffer_len = 0;
    struct sockaddr_in client_sin_addr{};
};

int64_t getSysTimeMs();
static void SendOutOfOrderBufferList(bool send_now);
void session_enable_write(TcpConnectInfo *session, bool to_server);
bool bind_client(TcpConnectInfo *tcp_connect_info, ssize_t nRead, const uv_buf_t *buf);
void close_connect(TcpConnectInfo* tcp_connect_info, bool is_server);

uint8_t udpRecvBuf[UDP_RECV_BUFFER_LEN];

const static int TCP_RECV_BUFFER_LEN = 1024 * 1024 * 1; // 1MB
uint8_t tcpRecvBuf[TCP_RECV_BUFFER_LEN];

uv_loop_t *loop = nullptr;
bool g_debug = false;

std::string server_ip = "0.0.0.0";
int server_port = 10000;

std::string tcp_server_ip = "0.0.0.0";
int tcp_server_port = 10000;

bool enable_out_order_send_udp = false;

int64_t now_ms = 0;
int64_t time_out_ms = 3000;
int64_t udp_time_out_ms = 500;

uint8_t heart_bit_buf[2];
uv_udp_t server_udp_watcher;
uv_tcp_t server_tcp_watcher;
int order_size = 3;

unordered_set<TcpConnectInfo *> wait_for_tcp;
unordered_set<TcpConnectInfo *> wait_for_bind;
unordered_map<uint64_t, PreparationInfo *> to_be_used_rts_server;
unordered_map<int64_t, ConnectInfo *> connect_groups;
unordered_set<ConnectInfo *> erase_set;
list<OutOfOrderBuffer *> OutOfOrderBufferList;

void alloc_cb_udp(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
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
    uv_buf_t bufs[1];
    bufs[0].base = buf->base;
    bufs[0].len = size;

    uint64_t key = UDP_KEY(((struct sockaddr_in *) addr)->sin_port, ((struct sockaddr_in *) addr)->sin_addr.s_addr);
    if (size == 2 && buf->base[0] == 'O' && buf->base[1] == 'K') {
        auto fv = to_be_used_rts_server.find(key);
        if (fv != to_be_used_rts_server.end()) {
            fv->second->last_recv_time = now_ms;
            return;
        }
        auto fv1 = connect_groups.find(key);
        if (fv1 != connect_groups.end()) {
            return;
        }
        char dst[18] = {0};
        uv_ip_name(addr, dst, 18);
        printf("add udp proxy [%s:%u] %lu %lu\n", dst, ntohs(((struct sockaddr_in *) addr)->sin_port),
               connect_groups.size(), to_be_used_rts_server.size());
        to_be_used_rts_server.insert({key, new PreparationInfo(*((struct sockaddr_in *) addr), now_ms)});
        return;
    }

    auto fv = connect_groups.find(key);
    if (fv == connect_groups.end()) {
        if (to_be_used_rts_server.empty()) {
            return;
        }

        auto *info = new ConnectInfo;
        info->last_recv_time = now_ms;
        info->client_key = key;
        info->rts_key = to_be_used_rts_server.begin()->first;
        info->dest_sin_addr = to_be_used_rts_server.begin()->second->udp_addr;
        info->client_sin_addr = *(struct sockaddr_in *) addr;

        delete to_be_used_rts_server.begin()->second;
        to_be_used_rts_server.erase(to_be_used_rts_server.begin());

        connect_groups.insert({info->rts_key, info});
        connect_groups.insert({info->client_key, info});
        uv_udp_try_send(&server_udp_watcher, bufs, 1, (struct sockaddr *) &info->dest_sin_addr);
        char dst[18] = {0};
        char rts_dst[18] = {0};
        uv_ip_name(addr, dst, 18);
        uv_ip_name((struct sockaddr *) &info->dest_sin_addr, rts_dst, 18);

        printf("bind client [%s:%u] to [%s:%u]\n", dst, ntohs(((struct sockaddr_in *) addr)->sin_port), rts_dst,
               ntohs(info->dest_sin_addr.sin_port));
        return;
    }

    fv->second->last_recv_time = now_ms;
    if (key == fv->second->client_key) {
        fv->second->recv_byte += size;
        uv_udp_try_send(&server_udp_watcher, bufs, 1, (struct sockaddr *) &fv->second->dest_sin_addr);
    } else {
        fv->second->send_byte += size;
        if (!enable_out_order_send_udp) {
            uv_udp_try_send(&server_udp_watcher, bufs, 1, (struct sockaddr *) &fv->second->client_sin_addr);
            return;
        }
        auto *buffer = new OutOfOrderBuffer;
        bufs[0].base = buf->base;
        bufs[0].len = size;
        memcpy(buffer->buffer, buf->base, size);
        buffer->buffer_len = size;
        buffer->client_sin_addr = fv->second->client_sin_addr;
        OutOfOrderBufferList.push_back(buffer);
        SendOutOfOrderBufferList(false);
    }
}

static void SendOutOfOrderBufferList(bool send_now = false) {
    while (OutOfOrderBufferList.size() > order_size || (send_now && !OutOfOrderBufferList.empty())) {
        auto *fv = OutOfOrderBufferList.back();

        uv_buf_t uvBuf[1];
        uvBuf[0].base = (char *) fv->buffer;
        uvBuf[0].len = fv->buffer_len;
        uv_udp_try_send(&server_udp_watcher, uvBuf, 1, (struct sockaddr *) &fv->client_sin_addr);

        OutOfOrderBufferList.pop_back();
    }
}

static void fun_log_timer(uv_timer_t *handle) {
    if (connect_groups.empty()) {
        return;
    }
    for (auto v = connect_groups.begin(); v != connect_groups.end(); v++) {
        char dst[18] = {0};
        char rts_dst[18] = {0};
        uv_ip_name((struct sockaddr *) &v->second->client_sin_addr, dst, 18);
        uv_ip_name((struct sockaddr *) &v->second->dest_sin_addr, rts_dst, 18);
        printf("bind client [%s:%u] to [%s:%u] send byte:%d recv byte:%d\n", dst,
               ntohs(((struct sockaddr_in *) &v->second->client_sin_addr)->sin_port), rts_dst,
               ntohs(v->second->dest_sin_addr.sin_port),
               v->second->send_byte, v->second->recv_byte
        );
    }
}

static void fun_send_order_timer(uv_timer_t *handle) {
    if (enable_out_order_send_udp) {
        SendOutOfOrderBufferList(true);
    }
}

static void check_and_bind_client() {
    if (wait_for_bind.empty()) {
        return;
    }
    for (auto v = wait_for_bind.begin(); v != wait_for_bind.end();) {
        if (bind_client(*v, 0, nullptr)) {
            v = wait_for_bind.erase(v);
            continue;
        }
        break;
    }
}

static void fun_check_timer(uv_timer_t *handle) {
    now_ms = getSysTimeMs();

    for (auto v = connect_groups.begin(); v != connect_groups.end();) {
        if (now_ms - v->second->last_recv_time <= time_out_ms) {
            v++;
            continue;
        }
        printf("time out clear\n");
        erase_set.insert(v->second);
        v = connect_groups.erase(v);
    }

    for (auto v = to_be_used_rts_server.begin(); v != to_be_used_rts_server.end();) {
        if (now_ms - v->second->last_recv_time <= udp_time_out_ms) {
            v++;
            continue;
        }
        delete v->second;
        v = to_be_used_rts_server.erase(v);
    }

    for (auto &v: erase_set) {
        char dst[18] = {0};
        char rts_dst[18] = {0};
        uv_ip_name((struct sockaddr *) &v->client_sin_addr, dst, 18);
        uv_ip_name((struct sockaddr *) &v->dest_sin_addr, rts_dst, 18);

        printf("close client [%s:%u] to [%s:%u]\n", dst, ntohs(v->client_sin_addr.sin_port), rts_dst,
               ntohs(v->dest_sin_addr.sin_port));
        delete v;
    }

    check_and_bind_client();

    erase_set.clear();
}

void write_cb(uv_write_t *req, int status) {
    if (status != 0) {
        printf("write_cb error\n");
        return;
    }
    auto *session = (TcpConnectInfo *) req->data;
    bool is_server = (req == &session->tcp_write_req_to_server);

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

void session_enable_write(TcpConnectInfo *session, bool to_server) {
    if (to_server) {
        if (session->send_queue_to_server.empty() || session->server_is_sending || session->server_close) {
            if (!session->server_is_sending && session->local_closed) {
                close_connect(session, to_server);
            }
            return;
        }
        session->server_is_sending = true;
        uv_buf_t bufs[1];
        bufs[0].base = reinterpret_cast<char *>(session->send_queue_to_server.front().data);
        bufs[0].len = session->send_queue_to_server.front().data_length;
        session->tcp_write_req_to_server.data = session;
        uv_write(&session->tcp_write_req_to_server, (uv_stream_t *) session->tcp_watcher_to_server, bufs, 1, write_cb);
    } else {
        if (session->send_queue_to_dest.empty() || session->local_is_sending || session->local_close) {
            if (!session->local_is_sending && session->server_closed) {
                close_connect(session, to_server);
            }
            return;
        }
        session->local_is_sending = true;
        uv_buf_t bufs[1];
        bufs[0].base = reinterpret_cast<char *>(session->send_queue_to_dest.front().data);
        bufs[0].len = session->send_queue_to_dest.front().data_length;
        session->tcp_write_req_to_dest.data = session;
        uv_write(&session->tcp_write_req_to_dest, (uv_stream_t *) session->tcp_watcher_to_dest, bufs, 1, write_cb);
    }
}

bool bind_client(TcpConnectInfo *tcp_connect_info, ssize_t nRead, const uv_buf_t *buf) {
    if (wait_for_tcp.empty()) {
        return false;
    }
    auto it = wait_for_tcp.begin();
    TcpConnectInfo *t2 = *it;
    wait_for_tcp.erase(it);
    t2->tcp_watcher_to_dest = tcp_connect_info->unidentified;
    tcp_connect_info->unidentified = nullptr;
    t2->tcp_watcher_to_dest->data = t2;

    while (!tcp_connect_info->send_queue_to_server.empty()) {
        t2->send_queue_to_server.push_back(tcp_connect_info->send_queue_to_server.front());
        tcp_connect_info->send_queue_to_server.pop_front();
    }

    if (nRead > 0 && buf) {
        NetBuffer buffer;
        buffer.data_length = nRead;
        buffer.data = new uint8_t[buffer.data_length];
        memcpy(buffer.data, buf->base, buffer.data_length);
        t2->send_queue_to_server.push_back(buffer);
    }
    session_enable_write(t2, true);
    delete tcp_connect_info;
    return true;
}

void close_connect(TcpConnectInfo* tcp_connect_info, bool is_server) {
    if (tcp_connect_info->unidentified) {
        wait_for_bind.erase(tcp_connect_info);
        uv_close(reinterpret_cast<uv_handle_t *>(tcp_connect_info->unidentified), [](uv_handle_t *handle) {
            delete (TcpConnectInfo *) handle->data;
        });
        return;
    }

    if (is_server) {
        if (tcp_connect_info->server_close || (tcp_connect_info->local_close && !tcp_connect_info->local_closed)) {
            return;
        }
        tcp_connect_info->server_close = true;
        uv_close(reinterpret_cast<uv_handle_t *>(tcp_connect_info->tcp_watcher_to_server), [](uv_handle_t *handle) {
            auto *tcp = (TcpConnectInfo *) handle->data;
            tcp->server_closed = true;
            if (tcp->tcp_watcher_to_dest) {
                if (tcp->send_queue_to_dest.empty() && !tcp->local_is_sending) {
                    uv_close(reinterpret_cast<uv_handle_t *>(tcp->tcp_watcher_to_dest), [](uv_handle_t *handle) {
                        delete (TcpConnectInfo *) handle->data;
                    });
                }
            } else {
                // 服务器没有绑定客户端直接删除
                wait_for_tcp.erase(tcp);
                delete tcp;
            }
        });
    } else {
        if (tcp_connect_info->local_close || (tcp_connect_info->server_close && !tcp_connect_info->server_closed)) {
            return;
        }
        tcp_connect_info->local_close = true;
        uv_close(reinterpret_cast<uv_handle_t *>(tcp_connect_info->tcp_watcher_to_dest), [](uv_handle_t *handle) {
            auto *tcp = (TcpConnectInfo *) handle->data;
            tcp->local_closed = true;
            if (tcp->send_queue_to_server.empty() && !tcp->server_is_sending) {
                uv_close(reinterpret_cast<uv_handle_t *>(tcp->tcp_watcher_to_server), [](uv_handle_t *handle) {
                    delete (TcpConnectInfo *) handle->data;
                });
            }
        });
    }
}

void tcp_cb(uv_stream_t *handle, ssize_t nRead, const uv_buf_t *buf) {
    auto *tcp_connect_info = (TcpConnectInfo *) handle->data;
    if (nRead <= 0) {
        printf("close_connect %d \n", handle == (uv_stream_t *) tcp_connect_info->tcp_watcher_to_server);
        close_connect(tcp_connect_info, handle == (uv_stream_t *) tcp_connect_info->tcp_watcher_to_server);
        return;
    }

    if (tcp_connect_info->unidentified) {
        if (tcp_connect_info->send_queue_to_server.empty() && nRead >= 2 && buf->base[0] == heart_bit_buf[0] &&
            buf->base[1] == heart_bit_buf[1]) {
            tcp_connect_info->tcp_watcher_to_server = tcp_connect_info->unidentified;
            tcp_connect_info->unidentified = nullptr;
            wait_for_tcp.insert(tcp_connect_info);
            check_and_bind_client();
            return;
        } else {
            if (wait_for_tcp.empty()) {
                NetBuffer buffer;
                buffer.data_length = nRead;
                buffer.data = new uint8_t[buffer.data_length];
                memcpy(buffer.data, buf->base, buffer.data_length);
                tcp_connect_info->send_queue_to_server.push_back(buffer);
                wait_for_bind.insert(tcp_connect_info);
            } else {
                wait_for_bind.erase(tcp_connect_info);
                bind_client(tcp_connect_info, nRead, buf);
            }
        }
    } else {
        NetBuffer buffer;
        buffer.data_length = nRead;
        buffer.data = new uint8_t[buffer.data_length];
        memcpy(buffer.data, buf->base, buffer.data_length);

        if (handle == (uv_stream_t *) tcp_connect_info->tcp_watcher_to_server) {
            tcp_connect_info->send_queue_to_dest.push_back(buffer);
            session_enable_write(tcp_connect_info, false);
        } else {
            tcp_connect_info->send_queue_to_server.push_back(buffer);
            session_enable_write(tcp_connect_info, true);
        }
    }
}

#define DEFAULT_BACKLOG 1000

void on_new_connection(uv_stream_t *server, int status) {
    if (status < 0) {
        fprintf(stderr, "New connection error %s\n", uv_strerror(status));
        return;
    }

    auto *client = (uv_tcp_t *) malloc(sizeof(uv_tcp_t));
    auto *tcp_connect_info = new TcpConnectInfo;
    tcp_connect_info->unidentified = client;
    uv_tcp_init(loop, client);
    client->data = tcp_connect_info;

    //判断accept是否成功
    if (uv_accept(server, (uv_stream_t *) client) == 0) {
        uv_read_start((uv_stream_t *) client, alloc_cb_tcp, tcp_cb);
    } else {
        uv_close((uv_handle_t *) client, [](uv_handle_t *handle) {
            delete (TcpConnectInfo*)handle->data;
        });
    }
}

bool tcp_server_init() {
    uv_tcp_init(loop, &server_tcp_watcher);

    struct sockaddr_in addr{};

    uv_ip4_addr(tcp_server_ip.c_str(), tcp_server_port, &addr);

    uv_tcp_bind(&server_tcp_watcher, (const struct sockaddr *) &addr, 0);

    int ret = uv_listen((uv_stream_t *) &server_tcp_watcher, DEFAULT_BACKLOG, on_new_connection);

    if (ret == 0) {
        printf("uv_listen ok \n");
    }
    return ret == 0;
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

void print_help(const char* name) {
    fprintf(stderr, "Usage: %s [OPTION]\n"
                    "\t-i, --udp_ip\n"
                    "\t-p, --udp_port\n"
                    "\t-I, --tcp_ip\n"
                    "\t-P, --tcp_port\n"
                    "\t-j, --just_udp\n"
                    "\t-J, --just_tcp\n"
                    "\t-d, --daemon\n"
                    "\t-D, --debug\n",name
    );
}

int main(int argc, char *argv[]) {
    int c;  //输入标记
    int help_flag = 0;
    bool is_daemon = false;

    bool enable_tcp = true;
    bool enable_udp = true;
    int ot = 10;

    struct option longOpts[] =
            {
                    {"udp_ip",       required_argument, 0,          'i'},
                    {"udp_port",     required_argument, 0,          'p'},
                    {"tcp_ip",     required_argument, 0,          'I'},
                    {"tcp_port",   required_argument, 0,          'P'},
                    {"just_udp", no_argument,       0,          'j'},
                    {"just_tcp", no_argument,       0,          'J'},
                    {"daemon",   no_argument,       0,          'd'},
                    // 下面的没有
                    {"debug",    no_argument,       0,          'D'},
                    {"order",    no_argument,       0,          'O'},
                    {"out",      required_argument, 0,          't'},
                    {"out_order",required_argument, 0,          's'},
                    {"help", 0,                     &help_flag, 1},
                    {0,      0,                     0,          0}
            };

    //有：表示有参数，两个：表示参数可选
    while ((c = getopt_long(argc, argv, "i:p:dDhI:P:Ojt:s:J?", longOpts, nullptr)) != EOF) {
        switch (c) {
            case 'i':
                server_ip = optarg;
                break;
            case 'p':
                server_port = atoi(optarg);
                break;
            case 'I':
                tcp_server_ip = optarg;
                break;
            case 'P':
                tcp_server_port = atoi(optarg);
                break;
            case 'O':
                enable_out_order_send_udp = true;
                break;
            case 't':
                ot = atoi(optarg);
                break;
            case 's':
                order_size = atoi(optarg);
                break;
            case 'j':
                enable_tcp = false;
                break;
            case 'J':
                enable_udp = false;
                break;
            case 'd':
                is_daemon = true;
                break;
            case 'D':
                g_debug = true;
                break;
            case 'h':
                help_flag = true;
                break;
            case '?':
                print_help(argv[0]);
                exit(0);
            default:
                break;
        }
    }

    if (help_flag || (!enable_udp && !enable_tcp)) {
        print_help(argv[0]);
        exit(0);
    }
    printf("server start udp[%s:%d] tcp[%s:%d]\n", server_ip.c_str(), server_port, tcp_server_ip.c_str(),
           tcp_server_port);

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
    uv_timer_start(&check_timer, fun_check_timer, 0, 50);

    uv_timer_t order_timer;
    if (enable_out_order_send_udp) {
        uv_timer_init(loop, &order_timer);
        uv_timer_start(&order_timer, fun_send_order_timer, 0, ot);
    }

    uv_timer_t log_timer;
    uv_timer_init(loop, &log_timer);
    uv_timer_start(&log_timer, fun_log_timer, 1000, 1000);

    int ret = 0;
    struct sockaddr_in sin{};

    if (enable_udp) {
        ret = uv_ip4_addr(server_ip.c_str(), server_port, &sin);
        if (ret != 0) return -1;
        do {
            ret = uv_udp_init(loop, &server_udp_watcher);
            if (ret != 0) break;

            ret = uv_udp_bind(&server_udp_watcher, (const struct sockaddr *) &sin, 0);
            if (ret != 0) break;

            ret = uv_udp_recv_start(&server_udp_watcher, alloc_cb_udp, udp_cb);
            if (ret != 0) break;
        } while (false);
        if (ret != 0) {
            uv_close(reinterpret_cast<uv_handle_t *>(&server_udp_watcher), [](uv_handle_t *handle) {
            });
            return -1;
        }
    }

    if (enable_tcp) {
        tcp_server_init();
    }

    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}


int64_t getSysTimeMs() {
    chrono::time_point<chrono::system_clock, chrono::milliseconds> tpMicro
            = chrono::time_point_cast<chrono::milliseconds>(chrono::system_clock::now());
    return tpMicro.time_since_epoch().count();
}
