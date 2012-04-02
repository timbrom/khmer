package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper extends Mapper<LongWritable, IntWritable, LongWritable, IntWritable>
{
    public void map(LongWritable key, IntWritable value, Context context)
            throws IOException, InterruptedException
    {
        context.write(key, value);
    }
}
