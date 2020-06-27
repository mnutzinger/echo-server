#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>
#include <boost/range/iterator_range_core.hpp>
#include <boost/system/error_code.hpp>

using std::atomic;
using std::enable_shared_from_this;
using std::function;
using std::lock_guard;
using std::make_shared;
using std::move;
using std::mutex;
using std::shared_ptr;
using std::string;
using std::string_view;
using std::thread;
using std::unordered_map;
using std::vector;

using boost::asio::buffer;
using boost::asio::const_buffer;
using boost::asio::error::operation_aborted;
using boost::asio::executor_work_guard;
using boost::asio::io_context;
using boost::asio::ip::tcp;
using boost::asio::make_strand;
using boost::asio::make_work_guard;
using boost::asio::signal_set;
using boost::make_iterator_range;
using boost::system::error_code;

atomic<size_t> numConnections = 0;
atomic<size_t> numRead = 0;
atomic<size_t> numReadLines = 0;
atomic<size_t> numWrite = 0;

class Connection final : public enable_shared_from_this<Connection> {
private:
    struct Tag {};

public:
    static shared_ptr<Connection> newConnection(tcp::socket conn,
                                                size_t maxReadBytes,
                                                bool process,
                                                function<void(Connection&)> onError)
    {
        auto ptr = make_shared<Connection>(move(conn),
                                           maxReadBytes,
                                           process,
                                           move(onError),
                                           Tag{});
        ptr->read_();

        return ptr;
    }

    Connection(tcp::socket conn,
               size_t maxReadBytes,
               bool process,
               function<void(Connection&)> onError,
               Tag) :
        conn_(move(conn)),
        process_(process),
        onError_(move(onError))
    {
        readBuf_.resize(maxReadBytes);
    }

    void close()
    {
        conn_.close();
    }

private:
    void read_()
    {
        conn_.async_read_some(
            buffer(readBuf_),
            [this, keep=shared_from_this()](const error_code& ec, size_t bytesTransferred) {
            if (ec) {
                onError_(*this);
            }
            else {
                ++numRead;
                const char* start = readBuf_.data();

                if (process_) {
                    readBuf_[bytesTransferred] = 0;
                    string_view sv(start, bytesTransferred);
                    auto pos = sv.find_first_of('\n');

                    size_t off = 0;

                    while (pos != string_view::npos) {
                        ++numReadLines;
                        writeQueue_.push_back(string(start+off, pos+1));

                        sv.remove_prefix(pos+1);
                        off += pos+1;
                        pos = sv.find_first_of('\n');
                    }

                    write_(writeQueue_.size());
                }
                else {
                    write_(buffer(start, bytesTransferred));
                }
            }
        });
    }

    void write_(size_t num)
    {
        if (num > writeBufs_.size()) {
            writeBufs_.resize(num);
        }

        for (size_t i = 0; i < num; ++i) {
            writeBufs_[i] = buffer(writeQueue_[i]);
        }

        write_(make_iterator_range(writeBufs_.begin(), writeBufs_.begin()+num));
    }

    template<class Buffers>
    void write_(const Buffers& bufs)
    {
        async_write(
            conn_,
            bufs,
            [this, keep=shared_from_this()](const error_code& ec, size_t bytesTransferred) {
            if (!ec) {
                ++numWrite;
                writeQueue_.clear();
                read_();
            }
        });
    }

    tcp::socket conn_;
    const bool process_{ false };
    function<void(Connection&)> onError_;
    string readBuf_;
    vector<string> writeQueue_;
    vector<const_buffer> writeBufs_;
};

class ConnectionManager final {
public:
    ConnectionManager(size_t maxReadBytes, bool process) :
        maxReadBytes_(maxReadBytes),
        process_(process)
    {}

    void addConnection(tcp::socket socket)
    {
        ++numConnections;

        int key = socket.remote_endpoint().port();
        auto conn = Connection::newConnection(move(socket),
                                              maxReadBytes_,
                                              process_,
                                              [this, key](Connection& conn) {
            conn.close();

            lock_guard<mutex> lock(mutex_);
            connections_.erase(key);
        });

        lock_guard<mutex> lock(mutex_);
        connections_.emplace(key, move(conn));
    }

    void disconnectAll()
    {
        lock_guard<mutex> lock(mutex_);

        for (auto& conn : connections_) {
            conn.second->close();
        }

        connections_.clear();
    }

private:
    const size_t maxReadBytes_{ 1024 };
    const bool process_{ false };
    mutex mutex_;
    unordered_map<int, shared_ptr<Connection>> connections_;
};

class ThreadedIoContext final {
public:
    explicit ThreadedIoContext(size_t numThreads) :
        work_(make_work_guard(io_))
    {
        while (numThreads-- > 0) {
            threads_.emplace_back([this]{
                io_.run();
            });
        }
    }

    ~ThreadedIoContext()
    {
        for (auto& t : threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    io_context& get()
    {
        return io_;
    }

    void stop()
    {
        work_.reset();
        io_.stop();
    }

private:
    io_context io_;
    executor_work_guard<io_context::executor_type> work_;
    vector<thread> threads_;
};

void acceptConnection(tcp::acceptor& acceptor,
                      io_context& io,
                      shared_ptr<ConnectionManager> connManager)
{
    acceptor.async_accept(
        make_strand(io),
        [&acceptor,
         &io,
         connManager=move(connManager)](const error_code& ec, tcp::socket socket) {
        if (!ec) {
            connManager->addConnection(move(socket));
        }

        acceptConnection(acceptor, io, move(connManager));
    });
}

int main(int argc, char* argv[])
{
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] <<
            " <port> <read buffer in KiB> <threads> [process]" << std::endl;
        return 1;
    }

    const int port = std::stoi(argv[1]);
    const size_t maxReadBytes = std::stoi(argv[2]) * 1024;
    const size_t numThreads = std::stoi(argv[3]);
    const bool process = (argc > 4) && !::strcmp(argv[4], "process");

    std::cout << "ping-pong Boost.Asio - " <<
        "port=" << port << ", " <<
        "maxReadBytes=" << maxReadBytes << " KiB, " <<
        "numThreads=" << numThreads << ", " <<
        "line processing=" << std::boolalpha << process << std::endl;

    ThreadedIoContext io(numThreads-1);

    signal_set signals(io.get(), SIGINT, SIGTERM);
    signals.async_wait([&io](const error_code& ec, int signum) {
        io.stop();
    });

    auto connManager = make_shared<ConnectionManager>(maxReadBytes, process);

    tcp::acceptor acceptor(io.get());
    tcp::endpoint ep(tcp::v4(), port);
    acceptor.open(ep.protocol());
    acceptor.set_option(tcp::acceptor::reuse_address(true));
    acceptor.bind(ep);
    acceptor.listen();
    acceptConnection(acceptor, io.get(), connManager);

    io.get().run();

    acceptor.close();
    connManager->disconnectAll();

    std::cout << "connections: " << numConnections <<
        ", read chunks: " << numRead <<
        ", read lines: " << numReadLines <<
        ", written: " << numWrite << std::endl;

    return 0;
}

