#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <zmq.hpp>

std::string receiveString(zmq::socket_t &socket) {
  zmq::message_t message;
  socket.recv(message, zmq::recv_flags::none);
  return message.to_string();
}

void sendString(zmq::socket_t &socket, const std::string &str) {
  zmq::message_t message(str.size());
  std::memcpy(message.data(), str.data(), str.size());
  socket.send(message, zmq::send_flags::none);
  // std::cerr << "    {SEND '" << str << "'}" << std::endl;
}

void sendMoreString(zmq::socket_t &socket, const std::string &str) {
  zmq::message_t message(str.size());
  std::memcpy(message.data(), str.data(), str.size());
  socket.send(message, zmq::send_flags::sndmore);
  // std::cerr << "    {SENDMORE " << str << "}" << std::endl;
}

// thread-safe printing
// print() calls printOne(), which recursively calls itself until all args
// are printed
// tout and terr are the user-facing entry points, which acquire a mutex for the
// right to output to either cout or cerr, respectively

std::ostream &printOne(std::ostream &os) { return os; }

template <class T, class... Args>
std::ostream &printOne(std::ostream &os, const T &t, const Args &...args) {
  os << t;
  return printOne(os, args...);
}

template <class... Args>
std::ostream &print(std::ostream &os, const Args &...args) {
  return printOne(os, args...);
}

std::mutex &getMutex() {
  static std::mutex mout;
  return mout;
}

template <class... Args>
std::ostream &tout(const Args &...args) {
  std::lock_guard<std::mutex> m(getMutex());
  return print(std::cout, args...);
}

template <class... Args>
std::ostream &terr(const Args &...args) {
  std::lock_guard<std::mutex> m(getMutex());
  return print(std::cerr, args...);
}
