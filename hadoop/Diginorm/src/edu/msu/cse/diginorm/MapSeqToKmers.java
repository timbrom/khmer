package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSeqToKmers extends Mapper<LongWritable, Text, Text, LongWritable>
{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        int K = context.getConfiguration().getInt("K", 32);
        // Split the value into K-Mers
        String seq = value.toString();
        int numKmers = seq.length() - K + 1;
        for (int i = 0; i < numKmers; i++)
        {
            context.write(new Text(seq.substring(i, i+K)), key);
        }
    }
}
