#include <string>
#include <stdint.h>
#include <iostream>

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

#include <counting.hh>
#include <primes.hh>

//TODO: Make hadoop distribute these values
#define MAX_MEDIAN_COUNT 10
#define TABLE_SIZE 200000000
#define KMER_SIZE 20

using namespace std;
using namespace khmer;

class DiginormMapper : public HadoopPipes::Mapper 
{
    public:
    DiginormMapper(HadoopPipes::TaskContext &context)
    {
    }

    void map(HadoopPipes::MapContext &context)
    {
        string key = context.getInputKey();
        string value = context.getInputValue();

        context.emit(key, value);
    }
};

class DiginormReducer : public HadoopPipes::Reducer
{
    public:
    CountingHash *h;
    long long unsigned int totalCount, keptCount;
    
    DiginormReducer(HadoopPipes::TaskContext &context)
    {
        Primes pri(TABLE_SIZE);
        vector<HashIntoType> tableSizes;
        tableSizes.push_back(pri.get_next_prime());
        tableSizes.push_back(pri.get_next_prime());
        tableSizes.push_back(pri.get_next_prime());
        h = new CountingHash(KMER_SIZE, tableSizes);
        totalCount = 0;
        keptCount = 0;
    }

    ~DiginormReducer()
    {
        printf("Total Count: %llu, Kept Count: %llu\n", totalCount, keptCount);
        printf("Hashtable Occupancy: %lf\n", (double)h->n_occupied() / TABLE_SIZE);
    }

    void reduce(HadoopPipes::ReduceContext &context) 
    {
        string key, value;
        while(context.nextValue())
        {
            BoundedCounterType medCount;
            float meanCount, stdDev;
            totalCount++;

            key = context.getInputKey();
            value = context.getInputValue();
            h->get_median_count(value, medCount, meanCount, stdDev);
            if (medCount < MAX_MEDIAN_COUNT)
            {
                keptCount++;
                KMerIterator kmers(value.c_str(), KMER_SIZE);
                while (!kmers.done())
                {
                    h->count(kmers.next());
                }
                context.emit(context.getInputKey(), context.getInputValue());
            }
        }
    }
};

int main(int argc, char **argv)
{
    return HadoopPipes::runTask(HadoopPipes::TemplateFactory<DiginormMapper, DiginormReducer>());
}
