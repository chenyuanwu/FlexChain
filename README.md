# legochain

## Hardware Requirements
At least 3 r320 instances on CloudLab.

## Software Dependencies
- libibverbs 
- leveldb
- gRPC

## Setup
### RDMA setup
1. Use packages that are shipped with the Linux distribution:
https://www.rdmamojo.com/2014/11/08/working-rdma-ubuntu/. We used Ubuntu 14-3.11.
2. Verify that RDMA on your machine it is working:
https://www.rdmamojo.com/2015/01/24/verify-rdma-working/


### leveldb setup
Install leveldb in the same directory where you clone our repo.
https://github.com/google/leveldb
```shell
$ git clone --recurse-submodules https://github.com/google/leveldb.git
$ mkdir -p build && cd build
$ cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```

### gRPC setup
Install gRPC in the same directory where you clone our repo.
Follow instructions in Section "Build and locally install gRPC and Protocol Buffers": https://grpc.io/docs/languages/cpp/quickstart/.

Several things to note: 
1. Do not set the ```$MY_INSTALL_DIR``` to be within your home directory when using CloudLab. CloudLab will erase everything in your home directory even when you take a snapshot of disk image.
2. Avoid using ```make -j```. 
3. Run the following command before compiling our code:
```shell
$ export PKG_CONFIG_PATH=$MY_INSTALL_DIR/lib/pkgconfig
$ export PATH=$PATH:$MY_INSTALL_DIR/bin
$ make
```

