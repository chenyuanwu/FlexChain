# FlexChain

## Hardware Requirements
At least 3 c6220 instances on CloudLab.

## Software Dependencies
- Ubuntu 18.04 or later versions
- libibverbs 
- leveldb
- gRPC

## Setup
### RDMA setup
1. Use packages that are shipped with the Linux distribution:
https://www.rdmamojo.com/2014/11/08/working-rdma-ubuntu/. Note: use the following command for Ubuntu 18.04 and later versions: 
```shell
$ sudo apt update
$ sudo apt-get -y --force-yes install build-essential cmake gcc \
libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config \
valgrind python3-dev cython3 python3-docutils pandoc libibverbs1 ibverbs-utils libibverbs-dev \
librdmacm1 rdmacm-utils librdmacm-dev libdapl2 ibsim-utils ibutils libibmad5 libibumad3 \
libmlx4-1 libmthca1 infiniband-diags mstflint opensm perftest srptools
```

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
1. (For CloudLab users) Do not set the ```$MY_INSTALL_DIR``` to be within your home directory. CloudLab will erase everything in your home directory even when you take a snapshot of disk image.
2. (For Ubuntu 18.04 users) Use Option A in the following post to update cmake: https://askubuntu.com/questions/355565/how-do-i-install-the-latest-version-of-cmake-from-the-command-line.
3. (For Ubuntu 18.04 users) Update your g++ to the latest versions 9.x.x: 
```shell
$ sudo apt install software-properties-common
$ sudo add-apt-repository ppa:ubuntu-toolchain-r/test
$ sudo apt install gcc-9 g++-9
```
4. Avoid using ```make -j```. 
5. Run the following command before compiling our code:
```shell
$ export PKG_CONFIG_PATH=$MY_INSTALL_DIR/lib/pkgconfig
$ export PATH=$PATH:$MY_INSTALL_DIR/bin
$ make
```

