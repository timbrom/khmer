/* This file is a simple serial implementation of the folloing algorithm:

for read in file:
    if median(read) > C:
        discard();
    else:
        count(read);
        save_to_file(read);
*/

#include <iostream>
#include <stdlib.h>
#include "parsers.hh"
#include "counting.hh"

using namespace std;
using namespace khmer;

#define K 32 /* kmer size */

#define MAX_MEDIAN_COUNT 20 /* Only count if the median is <= this */

/* Forward Declarations */

int main(int argc, char **argv)
{
    /* Check commandline arguments */
    if (argc != 2)
    {
        cout << "You must specify a filename" << endl;
        cout << "\tUsage: " << argv[0] << " filename" << endl;
        exit(-1);
    }

    /* Initialize the counting hash */
    vector<HashIntoType> tableSizes;
    tableSizes.push_back(100000000);
    tableSizes.push_back(100000001);
    tableSizes.push_back(100000002);
    CountingHash h(K, tableSizes);

    /* Start parsing the fasta file */
    FastaParser p(argv[1]);
    Read r;
    BoundedCounterType medCount;
    float meanCount;
    float stdDev;
    long long unsigned int totalCount = 0, keptCount = 0;

    while (!p.is_complete())
    {
        totalCount++;
        r = p.get_next_read();
        h.get_median_count(r.seq, medCount, meanCount, stdDev);
        if (medCount <= MAX_MEDIAN_COUNT)
        {
            keptCount++;
            /* Count it */
            KMerIterator kmers(r.seq.c_str(), K);
            while(!kmers.done())
            {
                h.count(kmers.next());
            }

            /* Save it to an output file */
            
        }
    }

    printf("Total Count: %llu, Kept Count: %llu\n", totalCount, keptCount);
    printf("Hashtable Occupancy: %lf\n", h.n_occupied() / 100000000.0);
}
