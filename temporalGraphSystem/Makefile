CCOMPILE=mpic++
PLATFORM=Linux-amd64-64

HADOOP_HOME=/usr/local/hadoop

CPPFLAGS= -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux  -I ~/yuzhen/pregelOL-code
LIB = -L$(HADOOP_HOME)/c++/$(PLATFORM)/lib
LDFLAGS = -lhdfs -Wno-deprecated -O2

all: run

run: run.cpp
	$(CCOMPILE) -std=c++11 tgs.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o run
	./kill.py
	
clean:
	-rm run
