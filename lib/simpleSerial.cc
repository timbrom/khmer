/* This file is a simple serial implementation of the folloing algorithm:

for read in file:
    if median(read) > C:
        discard();
    else:
        count(read);
        save_to_file(read);
*/

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include "parsers.hh"
#include "counting.hh"
#include "primes.hh"
#include <math.h>

using namespace std;
using namespace khmer;

#define MAX_MEDIAN_COUNT 10 /* Only count if the median is < this */
#define TABLE_SIZE 2000000000

/* Forward Declarations */

int main(int argc, char **argv)
{
    int numReads;
    int numKeptReads;

    /* Check commandline arguments */
    if (argc != 4)
    {
        cerr << "You must specify an input and output filename and the k-size" << endl;
        cerr << "\tUsage: " << argv[0] << " input filename output filename k-size" << endl;
        exit(-1);
    }

    ofstream outFile;
    outFile.open(argv[2]);
    if (! outFile.is_open())
    {
        cerr << "Failed to open file " << argv[2];
        perror("");
        cerr << endl;
        exit(-1);
    }

    unsigned int K;
    if (sscanf(argv[3], "%d", &K) != 1)
    {
        cerr << "Could not interpret K size of " << argv[3] << endl;
        exit(-1);
    }
    

    /* Initialize the counting hash */
    vector<HashIntoType> tableSizes;
    Primes pri(TABLE_SIZE);
    tableSizes.push_back(pri.get_next_prime());
    tableSizes.push_back(pri.get_next_prime());
    tableSizes.push_back(pri.get_next_prime());
    CountingHash h(K, tableSizes);

    /* Start parsing the fasta file */
    FastaParser p(argv[1]);
    Read r;
    BoundedCounterType medCount;
    float meanCount;
    float stdDev;
    long long unsigned int totalCount = 0, keptCount = 0;

    numReads = 0;
    numKeptReads = 0;
    int numThousands = 1;
    int numZeroCount;
    unsigned long long avgMedCount = 0;

    printf("ID\tNumKept  \tNumRead  \tNumZeroMed\tavgMedCnt\thtOccupancy\n");

    while (!p.is_complete())
    {
        numReads++;
        totalCount++;
        r = p.get_next_read();
        h.get_median_count(r.seq, medCount, meanCount, stdDev);
        avgMedCount += medCount;
        if (medCount < MAX_MEDIAN_COUNT)
        {
            keptCount++;
            numKeptReads++;
            if (medCount == 0)
                numZeroCount++;
            /* Count it */
            KMerIterator kmers(r.seq.c_str(), K);
            while(!kmers.done())
            {
                h.count(kmers.next());
            }

            /* Save it to an output file */
            outFile << ">" << r.name << endl;
            outFile << r.seq << endl;
        }

        if (numReads == 1000000)
        {
            printf("%-4d\t%-10d\t%-10d\t%-10d\t%-10.2lf\t%-10lf\n", numThousands, 
                numKeptReads, numReads, numZeroCount, 
                (double)avgMedCount / (double)numReads,
                (double)h.n_occupied() / TABLE_SIZE);
            numThousands++;
            numKeptReads = numReads = numZeroCount = avgMedCount = 0;
        }
    }

    outFile.close();

    printf("Total Count: %llu, Kept Count: %llu\n", totalCount, keptCount);
    printf("Hashtable Occupancy: %lf\n", (double)h.n_occupied() / TABLE_SIZE);
}
