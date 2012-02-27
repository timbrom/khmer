package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class FastaInputFormat extends FileInputFormat<LongWritable, Text>
{
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new FastaRecordReader();
    }

}
