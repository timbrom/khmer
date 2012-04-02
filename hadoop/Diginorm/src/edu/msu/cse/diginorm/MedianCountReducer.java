package edu.msu.cse.diginorm;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianCountReducer extends Reducer<LongWritable, IntWritable, Text, NullWritable>
{
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
    {
        ArrayList<Integer> cache = new ArrayList<Integer>();
        int C = context.getConfiguration().getInt("C", 32);
        
        for (IntWritable value : values)
        {
            cache.add(value.get());
        }
        Collections.sort(cache);
        
        // If Median is less than C, keep it. If not, pitch it.
        if (cache.get(cache.size() / 2) <= C)
        {
            context.write(new Text(key.toString()), NullWritable.get());
        }
    }
}

