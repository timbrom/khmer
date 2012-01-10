CC = g++
CPPFLAGS = -Wall -O2 -I$(HADOOP_INSTALL)/c++/Linux-amd64-64/include
LDFLAGS = -L$(HADOOP_INSTALL)/c++/Linux-amd64-64/lib

bin/diginorm_mapper: src/Diginorm.cpp
	$(CC) $(CPPFLAGS) $(LDFLAGS) $< -lhadooppipes -lhadooputils -lpthread -lcrypto -lssl -o $@

