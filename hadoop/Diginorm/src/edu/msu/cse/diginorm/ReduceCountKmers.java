package edu.msu.cse.diginorm;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceCountKmers extends Reducer<Text, Text, Text, IntWritable>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        ArrayList<Text> cache = new ArrayList<Text>();
        
        for (Text value : values)
        {
            cache.add(new Text(value)); // This is because Hadoop will re-use the old object vs. creating a new one
        }
        IntWritable abundCount = new IntWritable(cache.size());
        
        for(Text t:cache)
        {
            context.write(t, abundCount);
        }
    }
}
