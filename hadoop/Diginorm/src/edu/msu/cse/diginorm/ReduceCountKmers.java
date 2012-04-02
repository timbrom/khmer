package edu.msu.cse.diginorm;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceCountKmers extends Reducer<Text, LongWritable, LongWritable, IntWritable>
{
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
    {
        ArrayList<LongWritable> cache = new ArrayList<LongWritable>();
        
        for (LongWritable value : values)
        {
            cache.add(new LongWritable(value.get())); // This is because Hadoop will re-use the old object vs. creating a new one
        }
        IntWritable abundCount = new IntWritable(cache.size());
        
        for(LongWritable t:cache)
        {
            context.write(t, abundCount);
        }
    }
}
