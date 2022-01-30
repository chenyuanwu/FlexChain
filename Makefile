CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++17 -ggdb3
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
		   -L./leveldb/build -lleveldb\
           -pthread -lrdmacm -libverbs\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`
PROTOS_PATH = .


all: compute_server memory_server storage_server

compute_server: compute_server.cc setup_ib.o utils.o storage.pb.o storage.grpc.pb.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

memory_server: memory_server.cc setup_ib.o utils.o storage.pb.o storage.grpc.pb.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

storage_server: storage_server.cc storage.pb.o storage.grpc.pb.o 
	$(CXX) $(CPPFLAGS) -I./leveldb/include $(CXXFLAGS) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

.PHONY: clean
clean:
	rm -f *.o compute_server memory_server storage_server