cmake_minimum_required(VERSION 3.15)
project(udp_proxy)

set(CMAKE_CXX_STANDARD 14)
include_directories(
        /usr/local/include
)

link_libraries("/usr/local/lib/libuv.a")

add_executable(udp_proxy main.cpp)

target_link_libraries(udp_proxy pthread dl)