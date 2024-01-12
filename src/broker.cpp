#include <csignal>
#include <iostream>
#include <queue>
#include <random>
#include <string>
#include <zmq.hpp>

#include "zutil.h"

bool die = false;
int pollTime = 2000;

void signalHandler(int signal) {
  terr("<<SIGINT caught>>\n");
  die = true;
}

int main(int argc, char **argv) {
  std::signal(SIGINT, signalHandler);

  // single IO thread zmq context
  zmq::context_t context{1};

  // client-facing socket
  zmq::socket_t frontend{context, zmq::socket_type::router};
  // worker-facing socket
  zmq::socket_t backend{context, zmq::socket_type::router};
  backend.set(zmq::sockopt::router_mandatory, true);

  // clients connect over 9999
  frontend.bind("tcp://*:9999");
  // workers connect over 9998
  backend.bind("tcp://*:9998");

  std::string prefix = "[BROKER]";
  tout(prefix, " Bound\n");

  // LRU workers queue
  // LRU has the advantage over round robin that messages sent to a worker are
  // guaranteed to be sent to a worker free to work. no queue starts growing at
  // the worker
  std::queue<std::string> availableWorkers;

  while (!die) {
    zmq::pollitem_t items[] = {{backend, 0, ZMQ_POLLIN, 0},
                               {frontend, 0, ZMQ_POLLIN, 0}};

    try {
      // don't bother with client requests until we have at least one worker
      // ready. this will cause tasks to build up at the broker, which has to be
      // handled eventually
      if (availableWorkers.empty()) {
        // only poll for workers announcing that they're ready
        tout(prefix, " Waiting for workers...\n");
        zmq::poll(&items[0], 1, std::chrono::milliseconds(pollTime));
      } else {
        // poll for clients and workers
        tout(prefix, " Waiting for anyone...\n");
        zmq::poll(&items[0], 2, std::chrono::milliseconds(pollTime));
      }
    } catch (zmq::error_t &e) {
      // caught interrupt while polling, most likely place for ctrl-c to land
      terr(prefix, " Interrupted while polling\n");
      break;
    }

    // message over backend socket
    if (items[0].revents & ZMQ_POLLIN) {
      // receive a message from the worker - could be READY or a response
      std::string workerMessage, reply;

      try {
        // first get the worker's address
        // the worker's req socket prepended its response with this and an empty
        // frame
        std::string workerAddress = receiveString(backend);
        availableWorkers.push(workerAddress);

        // check the empty delimiter exists
        std::string empty = receiveString(backend);
        if (!empty.empty()) {
          terr(prefix, " Unexpected message structure (0); skipping\n");
          continue;
        }

        workerMessage = receiveString(backend);

        if (workerMessage == "READY") {
          tout(prefix, " Worker '", workerAddress, "' is ready\n");
        } else {
          // this is a response to a client, so we know that the contents of
          // workerMessage is the client's address
          empty = receiveString(backend);
          if (!empty.empty()) {
            terr(prefix, " Unexpected message structure (1); skipping\n");
            continue;
          }

          reply = receiveString(backend);
        }
      } catch (zmq::error_t &e) {
        // caught interrupt while receiving (somewhat unlikely)
        terr(prefix, " Interrupted while receiving from worker\n");
        break;
      }

      // finally, send the worker's response back to the client
      sendMoreString(frontend, workerMessage);
      sendMoreString(frontend, "");
      sendString(frontend, reply);

      tout(prefix, " Responded to client: '", workerMessage, "'\n");
    }
    // message over frontend socket
    else if (items[1].revents & ZMQ_POLLIN) {
      std::string clientAddress, request, assignedWorker;
      try {
        clientAddress = receiveString(frontend);
        std::string empty = receiveString(frontend);
        if (!empty.empty()) {
          terr(prefix, " Unexpected message structure (2); skipping\n");
          continue;
        }

        request = receiveString(frontend);
        assignedWorker = availableWorkers.front();
        availableWorkers.pop();
      } catch (zmq::error_t &e) {
        // caught interrupt while receiving (somewhat unlikely)
        terr(prefix, " Interrupted while receiving from client\n");
        break;
      }

      // send the assigned worker the message
      sendMoreString(backend, assignedWorker);
      sendMoreString(backend, "");
      sendMoreString(backend, clientAddress);
      sendMoreString(backend, "");
      sendString(backend, request);

      tout(prefix, " Request from client '", clientAddress, "' : '", request,
           "' sent to worker '", assignedWorker, "'\n");
    }
    // polling timed out
    else {
      tout("[BROKER] All is quiet...\n");
    }
  }

  tout(prefix, " Shutting down\n");

  tout(prefix, " Bye\n");

  return 0;
}
