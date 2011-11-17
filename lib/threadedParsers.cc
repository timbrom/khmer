#include "threadedParsers.hh"

/* This function returns the proper parser factory for a given
 * file type
 */
ThreadedIParserFactory* ThreadedIParserFactory::get_parser(const std::string &inputfile, long int chunkSize)
{
   std::string filename(inputfile);
   int found = filename.find_last_of(".");

   std::string type = filename.substr(found+1);

   if (type == "fq" || type == "fastq") {
      return new ThreadedFastqParserFactory(inputfile, chunkSize);
   } else if (type == "fa" || type == "fasta") {
      return new ThreadedFastaParserFactory(inputfile, chunkSize);
   } else {
      return new ThreadedFastaParserFactory(inputfile, chunkSize);
   }
}

/* Constructer for ThreadedFastaParserFactory */
ThreadedFastaParserFactory::ThreadedFastaParserFactory(
    const std::string &inputfile, long int size)
{
    /* Save off the filename and chunk size */
    filename = inputfile;
    chunkSize = size;
    curPos = 0;

    /* Get the file size */
    std::ifstream myFile;
    myFile.open(filename.c_str(), std::ifstream::in);
    myFile.seekg (0, std::ios::end);

    /* Subtract off one so that if there is only one
     * byte left (which is the case since we call unget)
     * we will still indicate that we have reached the end
     * of the file */
    fileSize = (long int)myFile.tellg() - 1;
    myFile.close();
}

ThreadedIParser* ThreadedFastaParserFactory::get_next_parser()
{
    /* Open the file */
    std::ifstream myFile;
    myFile.open(filename.c_str(), std::ifstream::in);
    long int myCurPos, endPos;

    do {
        /* Save off the current curPos so that we can use it in the compare
         * and swap later, to make this call threadsafe */
        myCurPos = curPos;

        /* Start the end position off at the current position + the chunk size */
        endPos = myCurPos + chunkSize;

        /* Read until we see
         * a '>', which indicates the start of the next read, or we reach EOF
         */
        myFile.seekg(endPos, std::ios::beg);

        while (!myFile.eof() && myFile.get() != '>');

        /* We get here as soon as we read a '>' */
        /* Put the '>' back */
        myFile.unget();
        endPos = myFile.tellg();

    } while(!__sync_bool_compare_and_swap(&curPos, myCurPos, endPos));
    

std::cout << "Returning fasta parser for " << filename << " Start pos: " << myCurPos << " End pos: " << endPos << std::endl;

    myFile.close();
    return new ThreadedFastaParser(filename, myCurPos, endPos);
}


/* Constructer for ThreadedFastqParserFactory */
ThreadedFastqParserFactory::ThreadedFastqParserFactory(
    const std::string &inputfile, long int size)
{
    /* Save off the filename and chunk size */
    filename = inputfile;
    chunkSize = size;
    curPos = 0;

    /* Get the file size */
    std::ifstream myFile;
    myFile.open(filename.c_str(), std::ifstream::in);
    myFile.seekg (0, std::ios::end);
    /* Subtract off one so that if there is only one
     * byte left (which is the case since we call unget)
     * we will still indicate that we have reached the end
     * of the file */
    fileSize = (long int)myFile.tellg() - 1;
    myFile.close();
}

ThreadedIParser* ThreadedFastqParserFactory::get_next_parser()
{
    /* Open the file */
    std::ifstream myFile;
    myFile.open(filename.c_str(), std::ifstream::in);
    long int myCurPos, endPos;

    do {
        /* Save off the current curPos so that we can use it in the compare
         * and swap later, to make this call threadsafe */
        myCurPos = curPos;

        /* Start the end position off at the current position + the chunk size */
        endPos = myCurPos + chunkSize;

        /* Read until we see
         * a '@', which indicates the start of the next read, or we reach EOF
         */
        myFile.seekg(endPos, std::ios::beg);

        while (!myFile.eof() && myFile.get() != '@');

        /* We get here as soon as we read a '@' */
        /* Put the '@' back */
        myFile.unget();
        endPos = myFile.tellg();

    } while(!__sync_bool_compare_and_swap(&curPos, myCurPos, endPos));
    

    myFile.close();
    return new ThreadedFastqParser(filename, myCurPos, endPos);
}



ThreadedFastaParser::ThreadedFastaParser(const std::string &inputfile, 
    long int startPos, long int end) : 
                         infile(inputfile.c_str())
{
    std::string line, seq;
    next_name = "";

    assert(infile.is_open());

    /* Lay the start pointer down in the right location */
    infile.seekg(startPos, std::ios::beg);

    /* Set the end ptr */
    endPos = end;

    bool valid_read = 0;

   while (!valid_read)  {
      line = "";
      seq = "";
      if (next_name == "")  {
        getline(infile, current_read.name);
        assert(current_read.name[0] == '>');
        current_read.name = current_read.name.substr(1);
      }
      else  {
         current_read.name = next_name;
         next_name = "";
      }
      
      while(line[0] != '>' && !infile.eof()) {
         getline(infile, line);
         if (line[0] != '>') {
            seq += line;
         }
      }

      if ((int)seq.find('N') == -1)  {
         valid_read = 1;
      }

      if (line[0] == '>') {
         next_name = line.substr(1);
      } else {
         seq += line;
      }

   }
    
   current_read.seq = seq;
}

Read ThreadedFastaParser::get_next_read()
{
   std::string line = "", seq = "";
   Read next_read = current_read;

   bool valid_read = 0;

   while (!valid_read)  {
      current_read.name = next_name;
      next_name = "";
      current_read.seq = "";

      getline(infile, line);

      while(line[0] != '>' && !infile.eof())
      {

         if (line[0] != '>')  {
            seq += line;
         }
         getline(infile, line);
      }

      if (line[0] == '>')  {
         next_name = line.substr(1);
      }

      if ((int)seq.find('N') == -1)  {
         valid_read = 1;
      }

      current_read.seq = seq;
      seq = "";

   }

   return next_read;
}

ThreadedFastqParser::ThreadedFastqParser(const std::string &inputfile, 
                long int startPos, long int end) :
                         infile(inputfile.c_str())
{
    std::string line_three, quality_scores;

    assert(infile.is_open());

    /* Lay the start pointer down in the right location */
    infile.seekg(startPos, std::ios::beg);

    /* Set the end ptr */
    endPos = end;

   bool valid_read = 0;

   while (!valid_read && !infile.eof())  {
      getline(infile, current_read.name);
      getline(infile, current_read.seq);
      getline(infile, line_three); 
      getline(infile, quality_scores);

      assert(current_read.name[0] == '@');
      assert(line_three[0] == '+' || line_three[0] == '#');
      assert(quality_scores.length() == current_read.seq.length());
   
      current_read.name = current_read.name.substr(1);

      if ((int)current_read.seq.find('N') == -1)  {
         valid_read = 1;
      }
   }

}

Read ThreadedFastqParser::get_next_read()
{
   Read next_read = current_read;
   std::string line_three, quality_scores;

   bool valid_read = 0;

   while (!valid_read && !infile.eof())  {

      getline(infile, current_read.name);

      if (infile.eof())  {
         return next_read;
      }

      getline(infile, current_read.seq);
      getline(infile, line_three);
      getline(infile, quality_scores);
   
      assert(current_read.name[0] == '@');
      assert(line_three[0] == '+' || line_three[0] == '#');
      assert(quality_scores.length() == current_read.seq.length());

      current_read.name = current_read.name.substr(1);

      if ((int)current_read.seq.find('N') == -1)  {
         valid_read = 1;
      }
   }

   return next_read;
}

