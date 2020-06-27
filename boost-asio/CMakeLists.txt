cmake_minimum_required(VERSION 3.12)

project(echo-server-boost-asio LANGUAGES CXX)

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)

find_package(Boost 1.70.0 REQUIRED)

add_executable(echo-server-boost-asio
    main.cpp
)

target_link_libraries(echo-server-boost-asio
PUBLIC Boost::boost
)

