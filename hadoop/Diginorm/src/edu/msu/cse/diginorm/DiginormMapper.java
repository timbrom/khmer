package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiginormMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        int K = context.getConfiguration().getInt("K", 32);
        System.out.println("K: " + K);
        
        context.write(value, key);
    }
}
