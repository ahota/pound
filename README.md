# ZeroMQ Load Balancer Example

Load Balancer = LB = pound

## Error handling

The broker and all threads can handle SIGINT (i.e. Ctrl-C) and shut down
gracefully.

## How to build

You need to have ZeroMQ (the base C library) already installed on your system
somewhere.

```bash
git clone git@github.com:ahota/pound.git
cd pound
git submodule init
git submodule update
mkdir build
cd build
cmake -DCPPZMQ_BUILD_TESTS=OFF ..
make
```
