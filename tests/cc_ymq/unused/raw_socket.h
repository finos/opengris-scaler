inline std::string as_byte_string(std::vector<uint8_t> buffer)
{
    std::stringstream ss {};
    for (auto x: buffer)
        ss << std::format("\\x{:02x}", x);
    return ss.str();
}

class RawSocket: public Socket {
    const uint16_t WINDOW_SZ = 65535;

    // local
    uint16_t _sport;
    in_addr_t _src;

    // remote
    uint16_t _dport;
    in_addr_t _dst;

    // the most recently sent seq and ack
    uint32_t _lseq;
    uint32_t _lack;

    uint32_t _rseq;
    uint32_t _rack;

public:
    RawSocket(): Socket(0), _sport(0), _src(0), _dport(0), _dst(0), _lseq(0), _lack(0), _rseq(0), _rack(0)
    {
        this->_fd = ::socket(AF_INET, SOCK_RAW, IPPROTO_TCP);

        if (this->_fd < 0)
            throw std::runtime_error("failed to create socket: " + std::to_string(errno));
    }

    void bind(const char* sAddr, int port)
    {
        // store the port and address in host order
        this->_sport = port;
        this->_src   = ntohl(inet_addr(check_localhost(sAddr)));
        this->Socket::bind(sAddr, port);
    }

    size_t send(void* buffer, size_t len, int flags = 0)
    {
        auto n = ::send(this->_fd, buffer, len, flags);

        if (n < 0)
            throw std::runtime_error("sendto() failed on raw socket: " + std::to_string(errno));
        return n;
    }

    void sendall(uint8_t* buffer, size_t len)
    {
        for (size_t cursor = 0; cursor < len;)
            cursor += this->send(buffer + cursor, len - cursor);
    }

    void sendall(std::vector<uint8_t> buffer) { return this->sendall(buffer.data(), buffer.size()); }

    // connect to a remote peer by performing a tcp handshake
    void connect(const char* sAddr, uint16_t port)
    {
        const size_t WINDOW = 4096;

        this->_lseq = rand() % 1000;
        this->Socket::connect(sAddr, port);

        // store the port and address in host order
        this->_dst = ntohl(inet_addr(check_localhost(sAddr)));
        if (this->_dst < 0)
            throw std::runtime_error("failed to get dst address");
        this->_dport = port;

        TcpMessage syn {
            .tcp =
                {.sport  = this->_sport,
                 .dport  = this->_dport,
                 .seq    = this->_lseq,
                 .ack    = 0,
                 .flags  = 0b00000010,  // SYN
                 .window = WINDOW,
                 .urgptr = 0},
        };
        this->write_raw_message(syn);

        auto response = this->read_raw_message();

        if (response.tcp.ack != syn.tcp.seq + 1)
            throw std::runtime_error(
                std::format("server returned a bad ack#, expected: {}, received: {}", syn.tcp.seq, response.tcp.ack));

        if (response.tcp.rst_flag())
            throw std::runtime_error("peer returned tcp rst: " + std::to_string(response.tcp.flags));

        if (!response.tcp.syn_flag() || !response.tcp.ack_flag())
            throw std::runtime_error("peer returned unexpected segment: flags: " + std::to_string(response.tcp.flags));

        std::println("recv seq: {}; options len: {}", response.tcp.seq, response.tcp.options.size());

        TcpMessage ack {
            .tcp =
                {.sport  = this->_sport,
                 .dport  = this->_dport,
                 .seq    = this->_rack,
                 .ack    = this->_rseq + 1,
                 .flags  = 0b00010000,  // ACK
                 .window = WINDOW,
                 .urgptr = 0},
        };
        this->write_raw_message(ack);
    }

    TcpMessage read_raw_message()
    {
        for (;;) {
            uint8_t buffer[4096] = {0};
            auto n               = this->read(buffer, 4096);
            if (n < 0)
                throw std::runtime_error("failed to read raw socket: " + std::to_string(errno));

            auto msg = TcpMessage::deserialize(PackedBuffer(buffer, n));
            if (msg.ipv4->mf_flag())
                throw std::runtime_error("ipv4 fragmentation is not supported");

            std::println("recv raw: {}", as_byte_string(std::vector(buffer, buffer + n)));
            if (msg.tcp.sport != this->_dport || msg.ipv4->src != this->_dst) {
                std::println("wrong: {}:{}", inet_ntoa({.s_addr = htonl(msg.ipv4->src)}), msg.tcp.sport);
                continue;
            }

            this->_rack = msg.tcp.ack;
            this->_rseq = msg.tcp.seq;
            return msg;
        }
    }

    void write_raw_message(TcpMessage msg)
    {
        this->_lseq  = msg.tcp.seq;
        this->_lack  = msg.tcp.ack;
        auto payload = msg.serialize(this->_src, this->_dst);
        std::println("write raw: {}", as_byte_string(payload));
        this->sendall(payload);
    }

    void write_message(std::vector<uint8_t> payload)
    {
        this->write_raw_message(
            {.tcp =
                 {.sport  = this->_sport,
                  .dport  = this->_dport,
                  .seq    = this->_lseq,
                  .ack    = this->_rseq + 1,
                  .flags  = 0b00011000,  // psh, ack
                  .window = 4096,
                  .urgptr = 0},
             .payload = payload});
    }

    void write_message(std::string msg)
    {
        this->write_message(std::vector<uint8_t>(msg.data(), msg.data() + msg.size()));
    }
};
