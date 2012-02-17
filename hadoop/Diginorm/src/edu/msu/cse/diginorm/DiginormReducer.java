package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiginormReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
    {
        for (LongWritable value : values)
            context.write(key, value);
    }
}
