#ifndef THREADED_PARSERS_H
#define THREADED_PARSERS_H

#include <iostream>
#include <string>
#include <string.h>
#include <fstream>
#include <assert.h>

struct Read
{
   std::string name;
   std::string seq;
   //std::string quality;
};

/* Base class for parsers, defines a set of methods that a parser must
 * implement and a static function that returns the proper parser
 * for different file types
 */
class ThreadedIParser
{
public:
   virtual Read get_next_read() = 0;
   virtual bool is_complete() = 0;
   virtual ~ThreadedIParser() { }
};

class ThreadedFastaParser : public ThreadedIParser
{
private:
   std::ifstream infile;
   long int endPos;
   Read current_read;
   std::string next_name;

public:
   ThreadedFastaParser(const std::string &inputfile, long int startPos, long int end);
   ~ThreadedFastaParser() { infile.close();  }
   Read get_next_read();
   bool is_complete() { return infile.tellg() >= endPos; } 
};

class ThreadedFastqParser : public ThreadedIParser
{
private:
   std::ifstream infile;
   long int endPos;
   Read current_read;

public:
   ThreadedFastqParser(const std::string &inputfile, long int startPos, long int end);
   ~ThreadedFastqParser() { infile.close(); }
   Read get_next_read();
   bool is_complete() { return infile.tellg() >= endPos; }
};

/* Base class for parser factories. Each factory will return a parser
 * object that will return all the reads within a chunk of the file
 */
class ThreadedIParserFactory
{
public:
    virtual ThreadedIParser* get_next_parser() = 0;
    virtual bool is_complete() = 0;
    static ThreadedIParserFactory* get_parser(const std::string &inputfile, long int chunkSize);
};

class ThreadedFastaParserFactory : public ThreadedIParserFactory
{
private:
    std::string filename;
    long int curPos;
    long int fileSize;
    long int chunkSize;

public:
    ThreadedFastaParserFactory(const std::string &inputfile, 
        long int size);
    ThreadedIParser* get_next_parser();
    bool is_complete() { return curPos >= fileSize; }
};

class ThreadedFastqParserFactory : public ThreadedIParserFactory
{
private:
    std::string filename;
    long int curPos;
    long int fileSize;
    long int chunkSize;

public:
    ThreadedFastqParserFactory(const std::string &inputfile, 
        long int chunkSize);
    ThreadedIParser* get_next_parser();
    bool is_complete() { return curPos >= fileSize; }
};

#endif
