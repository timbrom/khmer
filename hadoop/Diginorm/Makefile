CC = g++
CPPFLAGS = -Wall -O2 -I$(HADOOP_INSTALL)/c++/Linux-amd64-64/include
LDFLAGS = -L$(HADOOP_INSTALL)/c++/Linux-amd64-64/lib

all: bin/FastaParser.jar bin/diginorm_mapper

bin/diginorm_mapper: src/Diginorm.cpp
	$(CC) $(CPPFLAGS) $(LDFLAGS) $< -lhadooppipes -lhadooputils -lpthread -lcrypto -lssl -o $@

bin/FastaParser.jar: bin/edu/msu/cse/diginorm/FastaInputFormat.class bin/edu/msu/cse/diginorm/FastaRecordReader.class
	jar -cvf $@ $^

bin/edu/msu/cse/diginorm/FastaInputFormat.class: src/edu/msu/cse/diginorm/FastaInputFormat.java
	ant

clean:
	rm -rf bin
