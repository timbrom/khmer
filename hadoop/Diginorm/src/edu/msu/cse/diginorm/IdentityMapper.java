package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper extends Mapper<Text, IntWritable, Text, IntWritable>
{
    public void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException
    {
        context.write(key, value);
    }
}
