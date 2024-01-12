#include <csignal>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <zmq.hpp>

#include "zutil.h"

bool die = false;
int maxDelay = 2000;

void signalHandler(int signal) {
  terr("<<SIGINT caught>>\n");
  die = true;
}

void usage(const std::string message) {
  if (!message.empty()) std::cerr << message << std::endl;
  std::cerr << "Usage:" << std::endl;
  std::cerr << "  client -i ID -b BROKER_ADDRESS" << std::endl;
  exit(EXIT_FAILURE);
}

void usage(const char *message) { usage(std::string(message)); }

zmq::socket_t createSocket(zmq::context_t &context, std::string &brokerAddress) {
  zmq::socket_t socket(context, zmq::socket_type::req);
  // socket.set(zmq::sockopt::routing_id, clientID);
  socket.connect(brokerAddress);
  socket.set(zmq::sockopt::linger, 0);
  return socket;
}

int main(int argc, char **argv) {
  int id = -1;
  std::string brokerAddress;
  // get ID from command line (should change this, it's annoying)
  // get broker address from command line
  if (argc < 2) {
    usage("");
  } else {
    for (int i = 1; i < argc; i++) {
      if (std::string(argv[i]) == "-i") {
        if (++i < argc) {
          id = std::atoi(argv[i]);
          continue;
        } else {
          usage("Missing ID");
        }
      } else if (std::string(argv[i]) == "-b") {
        if (++i < argc) {
          brokerAddress = argv[i];
          continue;
        } else {
          usage("Missing broker address");
        }
      } else {
        usage(std::string("Invalid argument: ") + argv[i]);
      }
    }
  }
  if (id < 0) {
    usage("Invalid or missing ID");
  }
  if (brokerAddress.empty()) {
    usage("Missing broker address");
  }

  std::signal(SIGINT, signalHandler);

  std::string clientID("CLIENT"), prefix("[CLIENT] ");
  clientID.append(std::to_string(id));
  prefix.append(clientID);
  tout(prefix, " starting...\n");

  zmq::context_t context(1);
  zmq::socket_t socket = createSocket(context, brokerAddress);

  tout(prefix, " connected\n");
  std::random_device r;
  std::default_random_engine e(r());
  std::uniform_int_distribution<int> rng(100, maxDelay);

  // simulate work before messaging
  std::this_thread::sleep_for(std::chrono::milliseconds(rng(e)));

  sendString(socket, "HELLO");
  std::string reply;
  try {
    // try to get a response a few times before giving up
    bool keepTrying = true;
    int attemptsLeft = 5;
    while (keepTrying && !die) {
      zmq::pollitem_t item[] = {{socket, 0, ZMQ_POLLIN, 0}};
      // poll with timeout longer than the workers' max delay
      zmq::poll(&item[0], 1, std::chrono::milliseconds(maxDelay + 1000));
      if (item[0].revents & ZMQ_POLLIN) {
        // got message
        reply = receiveString(socket);
        keepTrying = false;
      } else if (attemptsLeft == 0) {
        // timed out too many times
        terr(prefix, " request timed out too many times\n");
        keepTrying = false;
      } else {
        // timed out
        terr(prefix, " request timed out; trying again (", attemptsLeft, ")\n");

        // delete the socket to close it and try again
        socket = createSocket(context, brokerAddress);
        sendString(socket, "HELLO");

        attemptsLeft--;
      }
    }
  } catch (zmq::error_t &e) {
    // interrupt caught while polling in this thread (unlikely)
    terr(prefix, " interrupted\n");
  }
  if (!reply.empty()) tout(prefix, " received '", reply, "'\n");
  tout(prefix, " shutting down\n");

  return EXIT_SUCCESS;
}
