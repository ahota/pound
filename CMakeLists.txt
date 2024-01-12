cmake_minimum_required(VERSION 3.8)

project(router-req LANGUAGES CXX VERSION 0.1)
set(CMAKE_CXX_STANDARD 14)

add_subdirectory(zmq)

add_executable(broker src/broker.cpp)
target_link_libraries(broker cppzmq)

add_executable(worker src/worker.cpp)
target_link_libraries(worker cppzmq)

add_executable(client src/client.cpp)
target_link_libraries(client cppzmq)
