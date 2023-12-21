#include <csignal>
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

// tell the worker threads to shut down
// only doing this because we're using threads for this example
// in reality, we would probably want the workers to stay alive and have
// a new broker come online
void shutdown(std::queue<std::string> &workers, zmq::socket_t &socket) {
  while (workers.size() > 0) {
    std::string w = workers.front();
    workers.pop();
    sendMoreString(socket, w);
    sendMoreString(socket, "");
    sendString(socket, "SHUTDOWN");
  }
}

// client thread
void client(int id) {
  std::string clientID("CLIENT"), prefix("[CLIENT] ");
  clientID.append(std::to_string(id));
  prefix.append(clientID);
  tout(prefix, " starting...\n");

  zmq::context_t context(1);
  zmq::socket_t socket(context, zmq::socket_type::req);
  socket.set(zmq::sockopt::routing_id, clientID);
  socket.connect("tcp://localhost:9999");

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
    while (keepTrying) {
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
        attemptsLeft--;
      }
    }
  } catch (zmq::error_t &e) {
    // interrupt caught while polling in this thread (unlikely)
    terr(prefix, " interrupted\n");
    return;
  }
  if (!reply.empty()) tout(prefix, " received '", reply, "'\n");
  tout(prefix, " shutting down\n");
}

// worker
void worker(int id) {
  std::string workerID("WORKER"), prefix("[WORKER] ");
  workerID.append(std::to_string(id));
  prefix.append(workerID);
  tout(prefix, " starting...\n");

  zmq::context_t context(1);
  zmq::socket_t socket(context, zmq::socket_type::req);
  socket.set(zmq::sockopt::routing_id, workerID);
  socket.connect("tcp://localhost:9998");

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

      // the broker caught an interrupt and is cleaning up
      if (address == "SHUTDOWN") {
        terr(prefix, " received shutdown signal\n");
        break;
      }

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

  // create some clients and workers
  std::vector<std::thread> clients, workers;
  for (int i = 0; i < 10; i++) {
    clients.push_back(std::thread(client, i));
  }
  for (int i = 0; i < 3; i++) {
    workers.push_back(std::thread(worker, i));
  }

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
        zmq::poll(&items[0], 1, std::chrono::milliseconds(maxDelay + 500));
      } else {
        // poll for clients and workers
        tout(prefix, " Waiting for anyone...\n");
        zmq::poll(&items[0], 2, std::chrono::milliseconds(maxDelay + 500));
      }
    } catch (zmq::error_t &e) {
      // caught interrupt while polling, most likely place for ctrl-c to land
      terr(prefix, " Interrupted while polling\n");
      shutdown(availableWorkers, backend);
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
        shutdown(availableWorkers, backend);
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
        shutdown(availableWorkers, backend);
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

  for (auto &t : clients) t.join();
  for (auto &t : workers) t.join();

  tout(prefix, " Bye\n");

  return 0;
}
