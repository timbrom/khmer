package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class FastaRecordReader extends RecordReader<LongWritable, Text>
{
    private LineRecordReader lineReader;
    private Text lineKey;
    private Text lineValue;
    private Text nextLineKey;
    private LongWritable linePos;
    private LongWritable nextLinePos;

    public FastaRecordReader(TaskAttemptContext context, FileSplit input) throws IOException, InterruptedException
    {
        lineReader = new LineRecordReader();
        
        lineKey = new Text();
        lineValue = new Text();
        nextLineKey = new Text(" ");
        linePos = new LongWritable();
        nextLinePos = new LongWritable();
    }

    @Override
    public void close() throws IOException
    {
        lineReader.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return linePos;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return lineValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return lineReader.getProgress();
    }

    @Override
    public void initialize(InputSplit input, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        lineReader.initialize(input, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {       
        /* Our key in Fasta is a line that starts with a '>'.
         * The key itself is the byte position of the '>' character 
         * Skip lines until we get to a line that starts with '>' 
         */
        while(nextLineKey.charAt(0) != '>')
        {
            if(lineReader.nextKeyValue() == false)
                return false;
            nextLineKey.set(lineReader.getCurrentValue());
            nextLinePos.set(lineReader.getCurrentKey().get());
        }
        
        /* We have our key. Save it off */
        lineKey.set(nextLineKey);
        linePos.set(nextLinePos.get());

        
        /* Now read the values (sequence data). There can be multiple 
         * lines of sequence data. Accumulate all lines until we reach a 
         * line that starts with a '>' which indicates the start of the 
         * next read
         */
        lineValue.clear();
        
        /* Get the next line */
        if(lineReader.nextKeyValue() == false)
            return false;
        nextLineKey.set(lineReader.getCurrentValue());
        nextLinePos.set(lineReader.getCurrentKey().get());
        
        while(nextLineKey.charAt(0) != '>')
        {
            lineValue.append(nextLineKey.getBytes(), 0, nextLineKey.getLength());
            /* We have hit the end */
            if(lineReader.nextKeyValue() == false)
                break;
            nextLineKey.set(lineReader.getCurrentValue());
            nextLinePos.set(lineReader.getCurrentKey().get());
        }
        
        /* We have successfully read if lineValue is greater than zero */
        return (lineValue.getLength() > 0);
    }
}