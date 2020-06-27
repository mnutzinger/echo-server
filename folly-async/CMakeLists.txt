cmake_minimum_required(VERSION 3.12)

project(echo-server-folly-async LANGUAGES CXX)

find_package(gflags REQUIRED)
find_package(folly REQUIRED)

add_executable(echo-server-folly-async
    main.cpp
)

target_link_libraries(echo-server-folly-async
PUBLIC Folly::folly
)

