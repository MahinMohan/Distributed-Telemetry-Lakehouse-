#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <rdkafka/rdkafka.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

using Clock = std::chrono::steady_clock;

static std::string getenv_str(const char *k, const std::string &def)
{
    const char *v = std::getenv(k);
    return (v && *v) ? std::string(v) : def;
}

static int getenv_int(const char *k, int def)
{
    const char *v = std::getenv(k);
    if (!v || !*v)
        return def;
    return std::atoi(v);
}

static int64_t now_unix_ms()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static std::string json_escape(const std::string &s)
{
    std::string out;
    out.reserve(s.size() + 16);
    for (char c : s)
    {
        switch (c)
        {
        case '\"':
            out += "\\\"";
            break;
        case '\\':
            out += "\\\\";
            break;
        case '\b':
            out += "\\b";
            break;
        case '\f':
            out += "\\f";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            if (static_cast<unsigned char>(c) < 0x20)
            {
                char buf[8];
                std::snprintf(buf, sizeof(buf), "\\u%04x", (int)(unsigned char)c);
                out += buf;
            }
            else
            {
                out += c;
            }
        }
    }
    return out;
}

struct TelemetryMsg
{
    std::string key;
    std::string value;
    int64_t ts_ms = 0;
};

class BoundedQueue
{
public:
    explicit BoundedQueue(size_t cap) : cap_(cap) {}

    bool push(TelemetryMsg &&m)
    {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_full_.wait(lk, [&]
                          { return closed_ || q_.size() < cap_; });
        if (closed_)
            return false;
        q_.emplace_back(std::move(m));
        cv_not_empty_.notify_one();
        return true;
    }

    bool pop(TelemetryMsg &out)
    {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_empty_.wait(lk, [&]
                           { return closed_ || !q_.empty(); });
        if (q_.empty())
            return false;
        out = std::move(q_.front());
        q_.pop_front();
        cv_not_full_.notify_one();
        return true;
    }

    void close()
    {
        std::lock_guard<std::mutex> lk(mu_);
        closed_ = true;
        cv_not_empty_.notify_all();
        cv_not_full_.notify_all();
    }

    bool closed() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        return closed_;
    }

private:
    size_t cap_;
    mutable std::mutex mu_;
    std::condition_variable cv_not_empty_;
    std::condition_variable cv_not_full_;
    std::deque<TelemetryMsg> q_;
    bool closed_ = false;
};

struct Counters
{
    std::atomic<uint64_t> in_msgs{0};
    std::atomic<uint64_t> in_bytes{0};
    std::atomic<uint64_t> out_msgs{0};
    std::atomic<uint64_t> out_bytes{0};
    std::atomic<uint64_t> dropped{0};
    std::atomic<uint64_t> delivery_err{0};
};

struct KafkaProducer
{
    rd_kafka_t *rk = nullptr;
    rd_kafka_conf_t *conf = nullptr;
    rd_kafka_topic_t *rkt = nullptr;

    std::string brokers;
    std::string topic;

    std::atomic<bool> running{true};
    Counters *ctr = nullptr;

    static void dr_msg_cb(rd_kafka_t *, const rd_kafka_message_t *rkmessage, void *opaque)
    {
        auto *self = static_cast<KafkaProducer *>(opaque);
        if (rkmessage->err)
        {
            self->ctr->delivery_err.fetch_add(1, std::memory_order_relaxed);
        }
    }

    bool init(const std::string &brokers_, const std::string &topic_)
    {
        brokers = brokers_;
        topic = topic_;

        char errstr[512];
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set_opaque(conf, this);
        rd_kafka_conf_set_dr_msg_cb(conf, &KafkaProducer::dr_msg_cb);

        auto set_conf = [&](const char *k, const std::string &v)
        {
            if (v.empty())
                return true;
            if (rd_kafka_conf_set(conf, k, v.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
            {
                std::cerr << "kafka conf error: " << k << ": " << errstr << "\n";
                return false;
            }
            return true;
        };

        if (!set_conf("bootstrap.servers", brokers))
            return false;
        if (!set_conf("acks", getenv_str("PRODUCER_ACKS", "1")))
            return false;
        if (!set_conf("enable.idempotence", getenv_str("PRODUCER_IDEMPOTENCE", "true")))
            return false;
        if (!set_conf("compression.type", getenv_str("PRODUCER_COMPRESSION", "lz4")))
            return false;
        if (!set_conf("linger.ms", getenv_str("PRODUCER_LINGER_MS", "5")))
            return false;
        if (!set_conf("batch.num.messages", getenv_str("PRODUCER_BATCH_NUM_MSGS", "10000")))
            return false;
        if (!set_conf("queue.buffering.max.kbytes", getenv_str("PRODUCER_QUEUE_MAX_KB", "1048576")))
            return false;
        if (!set_conf("message.timeout.ms", getenv_str("PRODUCER_MSG_TIMEOUT_MS", "30000")))
            return false;

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
        {
            std::cerr << "failed to create producer: " << errstr << "\n";
            return false;
        }

        rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);
        if (!rkt)
        {
            std::cerr << "failed to create topic handle: " << rd_kafka_err2str(rd_kafka_last_error()) << "\n";
            return false;
        }
        return true;
    }

    bool produce(const std::string &key, const std::string &payload)
    {

        int rc = rd_kafka_produce(
            rkt,
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            (void *)payload.data(),
            payload.size(),
            key.empty() ? nullptr : key.data(),
            key.size(),
            nullptr);

        if (rc != 0)
        {

            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL)
            {
                rd_kafka_poll(rk, 0);
                rc = rd_kafka_produce(
                    rkt,
                    RD_KAFKA_PARTITION_UA,
                    RD_KAFKA_MSG_F_COPY,
                    (void *)payload.data(),
                    payload.size(),
                    key.empty() ? nullptr : key.data(),
                    key.size(),
                    nullptr);
            }
        }

        rd_kafka_poll(rk, 0);
        return rc == 0;
    }

    void flush(int timeout_ms)
    {
        if (rk)
            rd_kafka_flush(rk, timeout_ms);
    }

    void shutdown()
    {
        running.store(false, std::memory_order_relaxed);
        flush(15000);
        if (rkt)
        {
            rd_kafka_topic_destroy(rkt);
            rkt = nullptr;
        }
        if (rk)
        {
            rd_kafka_destroy(rk);
            rk = nullptr;
        }
        conf = nullptr;
    }
};

static std::string make_envelope_json(const std::string &source,
                                      const std::string &host,
                                      int64_t ts_ms,
                                      const std::string &raw_line)
{

    std::string payload_escaped = json_escape(raw_line);
    std::string source_escaped = json_escape(source);
    std::string host_escaped = json_escape(host);

    std::string out;
    out.reserve(payload_escaped.size() + 128);
    out += "{";
    out += "\"ts_ms\":";
    out += std::to_string(ts_ms);
    out += ",\"source\":\"";
    out += source_escaped;
    out += "\",\"host\":\"";
    out += host_escaped;
    out += "\",\"payload\":\"";
    out += payload_escaped;
    out += "\"}";
    return out;
}

#ifdef _WIN32
static void winsock_init()
{
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
}
static void winsock_cleanup() { WSACleanup(); }
static void close_fd(SOCKET s) { closesocket(s); }
#else
static void close_fd(int fd) { ::close(fd); }
#endif

static std::string get_hostname()
{
#ifdef _WIN32
    char buf[256];
    DWORD sz = sizeof(buf);
    if (GetComputerNameA(buf, &sz))
        return std::string(buf);
    return "unknown";
#else
    char buf[256];
    if (gethostname(buf, sizeof(buf)) == 0)
        return std::string(buf);
    return "unknown";
#endif
}

static void run_udp_source(int port,
                           const std::string &source_name,
                           const std::string &host,
                           BoundedQueue &q,
                           Counters &ctr,
                           std::atomic<bool> &stop)
{
#ifdef _WIN32
    winsock_init();
    SOCKET sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock == INVALID_SOCKET)
    {
        std::cerr << "udp socket() failed\n";
        return;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sock, (sockaddr *)&addr, sizeof(addr)) != 0)
    {
        std::cerr << "udp bind() failed\n";
        close_fd(sock);
        winsock_cleanup();
        return;
    }
    std::vector<char> buf(65536);
    while (!stop.load(std::memory_order_relaxed))
    {
        int fromlen = sizeof(sockaddr_in);
        sockaddr_in from{};
        int n = recvfrom(sock, buf.data(), (int)buf.size(), 0, (sockaddr *)&from, &fromlen);
        if (n <= 0)
            continue;

        std::string line(buf.data(), buf.data() + n);
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r'))
            line.pop_back();

        TelemetryMsg m;
        m.ts_ms = now_unix_ms();
        m.key = host;
        m.value = make_envelope_json(source_name, host, m.ts_ms, line);

        ctr.in_msgs.fetch_add(1, std::memory_order_relaxed);
        ctr.in_bytes.fetch_add((uint64_t)m.value.size(), std::memory_order_relaxed);

        if (!q.push(std::move(m)))
            break;
    }
    close_fd(sock);
    winsock_cleanup();
#else
    int sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0)
    {
        std::perror("udp socket");
        return;
    }

    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sock, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        std::perror("udp bind");
        close_fd(sock);
        return;
    }

    std::vector<char> buf(65536);
    while (!stop.load(std::memory_order_relaxed))
    {
        sockaddr_in from{};
        socklen_t fromlen = sizeof(from);
        ssize_t n = recvfrom(sock, buf.data(), buf.size(), 0, (sockaddr *)&from, &fromlen);
        if (n < 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        std::string line(buf.data(), buf.data() + n);
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r'))
            line.pop_back();

        TelemetryMsg m;
        m.ts_ms = now_unix_ms();
        m.key = host;
        m.value = make_envelope_json(source_name, host, m.ts_ms, line);

        ctr.in_msgs.fetch_add(1, std::memory_order_relaxed);
        ctr.in_bytes.fetch_add((uint64_t)m.value.size(), std::memory_order_relaxed);

        if (!q.push(std::move(m)))
            break;
    }

    close_fd(sock);
#endif
}

static void run_tcp_source(int port,
                           const std::string &source_name,
                           const std::string &host,
                           BoundedQueue &q,
                           Counters &ctr,
                           std::atomic<bool> &stop)
{
#ifdef _WIN32
    winsock_init();
    SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listen_sock == INVALID_SOCKET)
    {
        std::cerr << "tcp socket() failed\n";
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listen_sock, (sockaddr *)&addr, sizeof(addr)) != 0)
    {
        std::cerr << "tcp bind() failed\n";
        close_fd(listen_sock);
        winsock_cleanup();
        return;
    }

    if (listen(listen_sock, 64) != 0)
    {
        std::cerr << "tcp listen() failed\n";
        close_fd(listen_sock);
        winsock_cleanup();
        return;
    }

    std::cerr << "tcp listening on port " << port << "\n";
    while (!stop.load(std::memory_order_relaxed))
    {
        SOCKET client = accept(listen_sock, nullptr, nullptr);
        if (client == INVALID_SOCKET)
            continue;

        std::string buffer;
        buffer.reserve(64 * 1024);
        std::vector<char> tmp(8192);

        while (!stop.load(std::memory_order_relaxed))
        {
            int n = recv(client, tmp.data(), (int)tmp.size(), 0);
            if (n <= 0)
                break;
            buffer.append(tmp.data(), tmp.data() + n);

            size_t pos = 0;
            while (true)
            {
                size_t nl = buffer.find('\n', pos);
                if (nl == std::string::npos)
                {
                    buffer.erase(0, pos);
                    break;
                }
                std::string line = buffer.substr(pos, nl - pos);
                if (!line.empty() && line.back() == '\r')
                    line.pop_back();

                TelemetryMsg m;
                m.ts_ms = now_unix_ms();
                m.key = host;
                m.value = make_envelope_json(source_name, host, m.ts_ms, line);

                ctr.in_msgs.fetch_add(1, std::memory_order_relaxed);
                ctr.in_bytes.fetch_add((uint64_t)m.value.size(), std::memory_order_relaxed);

                if (!q.push(std::move(m)))
                    break;

                pos = nl + 1;
            }
        }

        close_fd(client);
    }

    close_fd(listen_sock);
    winsock_cleanup();
#else
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        std::perror("tcp socket");
        return;
    }

    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listen_fd, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        std::perror("tcp bind");
        close_fd(listen_fd);
        return;
    }

    if (listen(listen_fd, 64) < 0)
    {
        std::perror("tcp listen");
        close_fd(listen_fd);
        return;
    }

    std::cerr << "tcp listening on port " << port << "\n";

    while (!stop.load(std::memory_order_relaxed))
    {
        int client = accept(listen_fd, nullptr, nullptr);
        if (client < 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        std::string buffer;
        buffer.reserve(64 * 1024);
        std::vector<char> tmp(8192);

        while (!stop.load(std::memory_order_relaxed))
        {
            ssize_t n = recv(client, tmp.data(), tmp.size(), 0);
            if (n <= 0)
                break;
            buffer.append(tmp.data(), tmp.data() + n);

            size_t pos = 0;
            while (true)
            {
                size_t nl = buffer.find('\n', pos);
                if (nl == std::string::npos)
                {
                    buffer.erase(0, pos);
                    break;
                }
                std::string line = buffer.substr(pos, nl - pos);
                if (!line.empty() && line.back() == '\r')
                    line.pop_back();

                TelemetryMsg m;
                m.ts_ms = now_unix_ms();
                m.key = host;
                m.value = make_envelope_json(source_name, host, m.ts_ms, line);

                ctr.in_msgs.fetch_add(1, std::memory_order_relaxed);
                ctr.in_bytes.fetch_add((uint64_t)m.value.size(), std::memory_order_relaxed);

                if (!q.push(std::move(m)))
                    break;

                pos = nl + 1;
            }
        }

        close_fd(client);
    }

    close_fd(listen_fd);
#endif
}

static void run_stdin_source(const std::string &source_name,
                             const std::string &host,
                             BoundedQueue &q,
                             Counters &ctr,
                             std::atomic<bool> &stop)
{
    std::string line;
    while (!stop.load(std::memory_order_relaxed) && std::getline(std::cin, line))
    {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        TelemetryMsg m;
        m.ts_ms = now_unix_ms();
        m.key = host;
        m.value = make_envelope_json(source_name, host, m.ts_ms, line);

        ctr.in_msgs.fetch_add(1, std::memory_order_relaxed);
        ctr.in_bytes.fetch_add((uint64_t)m.value.size(), std::memory_order_relaxed);

        if (!q.push(std::move(m)))
            break;
    }
}

static void run_synth_source(const std::string &source_name,
                             const std::string &host,
                             BoundedQueue &q,
                             Counters &ctr,
                             std::atomic<bool> &stop)
{
    int rate = getenv_int("SYNTH_RATE_HZ", 5000);
    if (rate < 1)
        rate = 1;
    auto period = std::chrono::microseconds(1000000 / rate);

    uint64_t seq = 0;
    while (!stop.load(std::memory_order_relaxed))
    {
        int64_t ts = now_unix_ms();
        std::string raw = "seq=" + std::to_string(seq++) + " cpu=0.42 mem=0.73 temp=67.1";
        TelemetryMsg m;
        m.ts_ms = ts;
        m.key = host;
        m.value = make_envelope_json(source_name, host, ts, raw);

        ctr.in_msgs.fetch_add(1, std::memory_order_relaxed);
        ctr.in_bytes.fetch_add((uint64_t)m.value.size(), std::memory_order_relaxed);

        if (!q.push(std::move(m)))
            break;

        std::this_thread::sleep_for(period);
    }
}

static void producer_loop(KafkaProducer &prod,
                          BoundedQueue &q,
                          Counters &ctr,
                          std::atomic<bool> &stop)
{
    const size_t batch_max_bytes = (size_t)std::max(1024, getenv_int("BATCH_MAX_BYTES", 1024 * 1024));
    const int batch_max_ms = std::max(1, getenv_int("BATCH_MAX_MS", 5));

    std::vector<TelemetryMsg> batch;
    batch.reserve(8192);

    size_t bytes = 0;
    auto last_flush = Clock::now();

    TelemetryMsg m;
    while (!stop.load(std::memory_order_relaxed))
    {
        bool got = q.pop(m);
        if (!got)
            break;

        bytes += m.value.size();
        batch.emplace_back(std::move(m));

        auto now = Clock::now();
        bool time_due = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush).count() >= batch_max_ms;
        bool size_due = bytes >= batch_max_bytes;

        if (time_due || size_due)
        {
            for (auto &item : batch)
            {
                bool ok = prod.produce(item.key, item.value);
                if (ok)
                {
                    ctr.out_msgs.fetch_add(1, std::memory_order_relaxed);
                    ctr.out_bytes.fetch_add((uint64_t)item.value.size(), std::memory_order_relaxed);
                }
                else
                {
                    ctr.dropped.fetch_add(1, std::memory_order_relaxed);
                }
            }
            batch.clear();
            bytes = 0;
            last_flush = now;
        }
    }

    for (auto &item : batch)
    {
        bool ok = prod.produce(item.key, item.value);
        if (ok)
        {
            ctr.out_msgs.fetch_add(1, std::memory_order_relaxed);
            ctr.out_bytes.fetch_add((uint64_t)item.value.size(), std::memory_order_relaxed);
        }
        else
        {
            ctr.dropped.fetch_add(1, std::memory_order_relaxed);
        }
    }

    prod.flush(15000);
}

static void metrics_loop(Counters &ctr, std::atomic<bool> &stop)
{
    using namespace std::chrono_literals;
    uint64_t last_in = 0, last_out = 0, last_in_b = 0, last_out_b = 0;

    while (!stop.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(1s);

        uint64_t in = ctr.in_msgs.load(std::memory_order_relaxed);
        uint64_t out = ctr.out_msgs.load(std::memory_order_relaxed);
        uint64_t in_b = ctr.in_bytes.load(std::memory_order_relaxed);
        uint64_t out_b = ctr.out_bytes.load(std::memory_order_relaxed);

        uint64_t din = in - last_in;
        uint64_t dout = out - last_out;
        uint64_t dinb = in_b - last_in_b;
        uint64_t doutb = out_b - last_out_b;

        last_in = in;
        last_out = out;
        last_in_b = in_b;
        last_out_b = out_b;

        std::cerr
            << "[1s] in=" << din << " msg/s (" << dinb / 1024.0 / 1024.0 << " MiB/s)"
            << " out=" << dout << " msg/s (" << doutb / 1024.0 / 1024.0 << " MiB/s)"
            << " dropped=" << ctr.dropped.load(std::memory_order_relaxed)
            << " delivery_err=" << ctr.delivery_err.load(std::memory_order_relaxed)
            << "\n";
    }
}

static std::atomic<bool> g_stop{false};

#ifndef _WIN32
static void on_sigint(int) { g_stop.store(true, std::memory_order_relaxed); }
#endif

int main()
{
#ifndef _WIN32
    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);
#endif

    std::string brokers = getenv_str("KAFKA_BROKERS", "localhost:9092");
    std::string topic = getenv_str("KAFKA_TOPIC", "telemetry.raw");
    std::string source = getenv_str("SOURCE", "udp");

    int udp_port = getenv_int("UDP_PORT", 9000);
    int tcp_port = getenv_int("TCP_PORT", 9001);

    size_t queue_cap = (size_t)std::max(1024, getenv_int("QUEUE_CAP", 200000));

    std::string host = get_hostname();
    std::string source_name = source;

    Counters ctr;
    BoundedQueue q(queue_cap);

    KafkaProducer prod;
    prod.ctr = &ctr;
    if (!prod.init(brokers, topic))
    {
        std::cerr << "failed to init kafka producer\n";
        return 1;
    }

    std::thread prod_t([&]
                       { producer_loop(prod, q, ctr, g_stop); });
    std::thread met_t([&]
                      { metrics_loop(ctr, g_stop); });

    std::thread src_t;
    if (source == "udp")
    {
        std::cerr << "source=udp port=" << udp_port << "\n";
        src_t = std::thread([&]
                            { run_udp_source(udp_port, source_name, host, q, ctr, g_stop); });
    }
    else if (source == "tcp")
    {
        std::cerr << "source=tcp port=" << tcp_port << "\n";
        src_t = std::thread([&]
                            { run_tcp_source(tcp_port, source_name, host, q, ctr, g_stop); });
    }
    else if (source == "stdin")
    {
        std::cerr << "source=stdin\n";
        src_t = std::thread([&]
                            { run_stdin_source(source_name, host, q, ctr, g_stop); });
    }
    else if (source == "synth")
    {
        std::cerr << "source=synth\n";
        src_t = std::thread([&]
                            { run_synth_source(source_name, host, q, ctr, g_stop); });
    }
    else
    {
        std::cerr << "unknown SOURCE: " << source << " (use udp|tcp|stdin|synth)\n";
        g_stop.store(true, std::memory_order_relaxed);
    }

    while (!g_stop.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    q.close();
    if (src_t.joinable())
        src_t.join();
    if (prod_t.joinable())
        prod_t.join();

    prod.shutdown();

    g_stop.store(true, std::memory_order_relaxed);
    if (met_t.joinable())
        met_t.join();

    std::cerr
        << "final: in_msgs=" << ctr.in_msgs.load()
        << " out_msgs=" << ctr.out_msgs.load()
        << " dropped=" << ctr.dropped.load()
        << " delivery_err=" << ctr.delivery_err.load()
        << "\n";
    return 0;
}