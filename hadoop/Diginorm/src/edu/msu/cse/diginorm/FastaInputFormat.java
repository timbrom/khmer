package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class FastaInputFormat extends FileInputFormat<Text, Text>
{
    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit input,
            JobConf job, Reporter reporter) throws IOException
    {
        reporter.setStatus(input.toString());
        return new FastaRecordReader(job, (FileSplit)input);
    }
}
