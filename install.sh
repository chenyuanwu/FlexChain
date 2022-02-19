#!/bin/bash
cd ~

# Install RDMA verbs
sudo apt update

sudo apt-get -y --force-yes install build-essential cmake gcc \
libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config \
valgrind python3-dev cython3 python3-docutils pandoc libibverbs1 ibverbs-utils libibverbs-dev \
librdmacm1 rdmacm-utils librdmacm-dev libdapl2 ibsim-utils ibutils libibmad5 libibumad3 \
libmlx4-1 libmthca1 infiniband-diags mstflint opensm perftest srptools

# Install leveldb
git clone --recurse-submodules https://github.com/google/leveldb.git
cd leveldb/
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
cd ~

# Install gRPC
export MY_INSTALL_DIR=$HOME/.local
mkdir -p $MY_INSTALL_DIR
export PATH="$MY_INSTALL_DIR/bin:$PATH"

sudo apt remove --purge --auto-remove cmake
sudo apt update && \
sudo apt install -y software-properties-common lsb-release && \
sudo apt clean all
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
sudo apt-add-repository "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main"
sudo apt update
sudo apt install cmake

git clone --recurse-submodules -b v1.43.0 https://github.com/grpc/grpc
cd grpc/
mkdir -p cmake/build
pushd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
make
make install
popd
cd ~

sudo apt install software-properties-common
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt install gcc-9 g++-9





