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
  std::cerr << "  worker -i ID -b BROKER_ADDRESS" << std::endl;
  exit(EXIT_FAILURE);
}

void usage(const char *message) { usage(std::string(message)); }

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

  std::string workerID("WORKER"), prefix("[WORKER] ");
  workerID.append(std::to_string(id));
  prefix.append(workerID);
  tout(prefix, " starting...\n");

  zmq::context_t context(1);
  zmq::socket_t socket(context, zmq::socket_type::req);
  socket.set(zmq::sockopt::routing_id, workerID);
  socket.connect(brokerAddress);

  std::random_device r;
  std::default_random_engine e(r());
  std::uniform_int_distribution<int> rng(100, 5000);

  // simulate setup work before declaring we're ready
  std::this_thread::sleep_for(std::chrono::milliseconds(rng(e)));

  tout(prefix, " connected\n");

  sendString(socket, "READY");

  while (!die) {
    std::string address, request;
    try {
      // the worker will block and wait until it receives a message; either from
      // a client or from the broker to shutdown.
      // we don't really want to poll+retry+fail like the client because we
      // don't want the worker to go down for a lack of work. it needs to stay
      // available.
      // one possibility here is to maintain a heartbeat with the broker to
      // confirm availabity
      address = receiveString(socket);

      std::string empty = receiveString(socket);
      if (!empty.empty()) {
        terr(prefix, " ERROR: Unexpected message structure; skipping\n");
        continue;
      }

      request = receiveString(socket);
    } catch (zmq::error_t &e) {
      // interrupt caught while polling in this thread (unlikely)
      terr(prefix, " interrupted\n");
      break;
    }
    tout(prefix, " received '", request, "' from client '", address, "'\n");

    // simulate working on the request
    std::this_thread::sleep_for(std::chrono::milliseconds(rng(e)));

    sendMoreString(socket, address);
    sendMoreString(socket, "");
    sendString(socket, "OK");
  }

  tout(prefix, " shutting down\n");

  return EXIT_SUCCESS;
}
