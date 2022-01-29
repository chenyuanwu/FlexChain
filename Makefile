# name of the program to build
#
PROG=bin/penn-os

PROMPT='"$$ "'

# Remove -DNDEBUG during development if assert(3) is used
#
override CPPFLAGS += -DNDEBUG -DPROMPT=$(PROMPT)

CC = clang

# Replace -O1 with -g for a debug version during development
#
CFLAGS = -Wall -Werror -g

FSTARGET = bin/pennfat
FSSRCS := $(wildcard src/fs/*.c)
FSSRCS := $(filter-out src/fs/pennfat.c, $(FSSRCS))
FSSRCS := $(filter-out src/fs/virtual_pcb.c, $(FSSRCS))
FSOBJS = $(FSSRCS:.c=.o)
PENNFAT_ENTRY := src/fs/virtual_pcb.o src/fs/pennfat.o 

SRCS = $(wildcard src/*.c)
OBJS = $(SRCS:.c=.o)

.PHONY : clean

all: fs shell

shell: $(PROG) 

fs: $(FSTARGET)

clean :
	$(RM) $(OBJS) $(PROG) $(FSOBJS) $(FSTARGET) $(PENNFAT_ENTRY)

$(PROG) : $(OBJS) $(FSOBJS)
	mkdir -p bin
	mkdir -p log
	$(CC) -o $@ -DKERNEL_CONTEXT $^ src/parsejob.o 

$(FSTARGET): $(FSOBJS) $(PENNFAT_ENTRY) 
	mkdir -p bin
	$(CC) -o $@ $^ src/parsejob.o 
	rm -rf src/fs/fat_internal.o src/fs/inode_internal.o  src/fs/inode_internal.o
	$(CC) $(CFLAGS) $(CPPFLAGS) -DKERNEL_CONTEXT -c -o src/fs/fat_internal.o src/fs/fat_internal.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DKERNEL_CONTEXT -c -o src/fs/inode_internal.o src/fs/inode_internal.c