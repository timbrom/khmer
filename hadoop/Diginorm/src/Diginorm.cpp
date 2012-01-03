#include <string>
#include <stdint.h>

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

using namespace std;

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

        if (value.find('N') != string::npos)
        {
            context.emit(key, value);
        }
    }
};

class DiginormReducer : public HadoopPipes::Reducer
{
    public:
    DiginormReducer(HadoopPipes::TaskContext &context)
    {
    //TODO: Setup for bloom filter
    }

    void reduce(HadoopPipes::ReduceContext &context) 
    {
        while(context.nextValue())
        {
            context.emit(context.getInputKey(), context.getInputValue());
        }
    }
};

int main(int argc, char **argv)
{
    return HadoopPipes::runTask(HadoopPipes::TemplateFactory<DiginormMapper, DiginormReducer>());
}
