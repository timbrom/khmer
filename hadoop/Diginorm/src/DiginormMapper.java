import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiginormMapper extends Mapper<Text, Text, Text, Text>
{
    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String qual = key.toString();
        String seq = value.toString();

        context.write(new Text(qual), new Text(seq));

    }
}

