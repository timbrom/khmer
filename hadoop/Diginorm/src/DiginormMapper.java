import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DiginormMapper extends MapReduceBase 
    implements Mapper<Text, Text, Text, Text>
{
    public void map(Text key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException
    {
        String qual = key.toString();
        String seq = value.toString();

        output.collect(new Text(qual), new Text(seq));

    }
}

