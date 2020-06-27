cmake_minimum_required(VERSION 3.12)

project(echo-server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(
    "-Wall"
    "-Werror"
)

add_subdirectory(boost-asio)
add_subdirectory(folly-async)

