cmake_minimum_required(VERSION 3.8)

project(router-req LANGUAGES CXX VERSION 0.1)
set(CMAKE_CXX_STANDARD 14)

add_subdirectory(zmq)

add_executable(router-req src/router-req.cpp)
target_link_libraries(router-req cppzmq)
