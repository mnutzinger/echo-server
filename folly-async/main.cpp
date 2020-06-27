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

#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseManager.h>

using std::atomic;
using std::exception;
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

using folly::AsyncServerSocket;
using folly::AsyncSignalHandler;
using folly::AsyncSocket;
using folly::AsyncSocketException;
using folly::EventBase;
using folly::EventBaseManager;
using folly::NetworkSocket;
using folly::SocketAddress;

atomic<size_t> numConnections = 0;
atomic<size_t> numRead = 0;
atomic<size_t> numReadLines = 0;
atomic<size_t> numWrite = 0;

class Connection final : public AsyncSocket::ReadCallback,
                         public AsyncSocket::WriteCallback {
public:
    Connection(EventBase* io,
               NetworkSocket fd,
               size_t maxReadBytes,
               bool process,
               function<void(Connection&)> onError) :
        conn_(AsyncSocket::newSocket(io, move(fd))),
        process_(process),
        onError_(move(onError))
    {
        readBuf_.resize(maxReadBytes);

        conn_->setReadCB(this);
    }

    void close()
    {
        conn_->setReadCB(nullptr);
        conn_->close();
    }

protected:
    void getReadBuffer(void** bufReturn, size_t* lenReturn) override
    {
        *bufReturn = readBuf_.data();
        *lenReturn = readBuf_.size();
    }

    void readDataAvailable(size_t len) noexcept override
    {
        ++numRead;
        const char* start = readBuf_.data();

        if (process_) {
            readBuf_[len] = 0;
            string_view sv(start, len);
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
            conn_->write(this, start, len);
        }
    }

    void readEOF() noexcept override
    {
        onError_(*this);
    }

    void readErr(const AsyncSocketException& ex) noexcept override
    {
        onError_(*this);
    }

    void writeSuccess() noexcept override
    {
        ++numWrite;
        writeQueue_.clear();
    }

    void writeErr(size_t nBytesWritten,
                  const AsyncSocketException& ex) noexcept override
    {}

private:
    void write_(size_t num)
    {
        if (num > writeOp_.size()) {
            writeOp_.resize(num);
        }

        for (size_t i = 0; i < num; ++i) {
            writeOp_[i].iov_base = writeQueue_[i].data();
            writeOp_[i].iov_len = writeQueue_[i].size();
        }

        conn_->writev(this, writeOp_.data(), num);
    }

    shared_ptr<AsyncSocket> conn_;
    const bool process_{ false };
    function<void(Connection&)> onError_;
    string readBuf_;
    vector<string> writeQueue_;
    vector<iovec> writeOp_;
};

class ConnectionManager final {
public:
    ConnectionManager(size_t maxReadBytes, bool process) :
        maxReadBytes_(maxReadBytes),
        process_(process)
    {}

    void addConnection(EventBase* io, NetworkSocket fd)
    {
        ++numConnections;

        int key = fd.toFd();
        auto conn = make_shared<Connection>(io,
                                            move(fd),
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

class Listener final : public AsyncServerSocket::AcceptCallback {
public:
    Listener(const vector<EventBase*>& io,
             int port,
             shared_ptr<ConnectionManager> connManager) :
        socket_(AsyncServerSocket::newSocket(io[0])),
        connManager_(move(connManager))
    {
        socket_->bind(port);
        socket_->listen(10);

        for (size_t i = 0; i < io.size(); ++i) {
            socket_->addAcceptCallback(this, io[i]);
        }
    }

    void start()
    {
        socket_->startAccepting();
    }

    void stop()
    {
        socket_->stopAccepting();
    }

protected:
    void connectionAccepted(NetworkSocket fd,
                            const SocketAddress& clientAddr) noexcept override
    {
        (void)clientAddr;
        connManager_->addConnection(EventBaseManager::get()->getEventBase(), move(fd));
    }

    void acceptError(const exception& ex) noexcept override
    {}

private:
    shared_ptr<AsyncServerSocket> socket_;
    shared_ptr<ConnectionManager> connManager_;
};

class SignalHandler : public AsyncSignalHandler {
public:
    SignalHandler(EventBase* io, const vector<int>& signums, function<void()> onSignal) :
        AsyncSignalHandler(io),
        onSignal_(onSignal)
    {
        for (int signum : signums) {
            registerSignalHandler(signum);
        }
    }

    void signalReceived(int signum) noexcept override
    {
        (void)signum;
        onSignal_();
    }

private:
    function<void()> onSignal_;
};

class EventBaseThreads final {
public:
    EventBaseThreads(size_t numThreads, size_t off) :
        io_(numThreads)
    {
        for (size_t i = off; i < numThreads; ++i) {
            threads_.emplace_back([this, i]{
                run(i);
            });
        }
    }

    ~EventBaseThreads()
    {
        for (auto& t : threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    EventBase* get()
    {
        static size_t next = 0;

        size_t i = next++;

        if (next >= io_.size()) {
            next = 0;
        }

        return &io_[i];
    }

    vector<EventBase*> getAll()
    {
        vector<EventBase*> ret(io_.size());

        for (size_t i = 0; i < io_.size(); ++i) {
            ret[i] = &io_[i];
        }

        return ret;
    }

    void run(size_t i)
    {
        if (i >= io_.size()) {
            return;
        }

        EventBaseManager::get()->setEventBase(&io_[i], false);
        io_[i].loopForever();
    }

    void stop()
    {
        for (EventBase& io : io_) {
            io.terminateLoopSoon();
        }
    }

private:
    vector<EventBase> io_;
    vector<thread> threads_;
};

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

    std::cout << "ping-pong Folly.Async - " <<
        "port=" << port << ", " <<
        "maxReadBytes=" << maxReadBytes << " KiB, " <<
        "numThreads=" << numThreads << ", " <<
        "line processing=" << std::boolalpha << process << std::endl;

    EventBaseThreads io(numThreads, 1);

    SignalHandler signalHandler(io.get(), { SIGINT, SIGTERM }, [&io]{
        io.stop();
    });

    auto connManager = make_shared<ConnectionManager>(maxReadBytes, process);

    auto listener = make_shared<Listener>(io.getAll(), port, connManager);
    listener->start();

    io.run(0);

    listener->stop();
    connManager->disconnectAll();

    std::cout << "connections: " << numConnections <<
        ", read chunks: " << numRead <<
        ", read lines: " << numReadLines <<
        ", written: " << numWrite << std::endl;

    return 0;
}

