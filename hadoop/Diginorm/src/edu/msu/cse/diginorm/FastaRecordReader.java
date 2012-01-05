package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class FastaRecordReader implements RecordReader<Text, Text>
{
    private LineRecordReader lineReader;
    private LongWritable lineKey;
    private Text lineValue;
    private Text nextKey;

    public FastaRecordReader(JobConf job, FileSplit input) throws IOException
    {
        lineReader = new LineRecordReader(job, input);

        lineKey = lineReader.createKey();
        lineValue = lineReader.createValue();
        nextKey = new Text(" ");
    }

    @Override
    public void close() throws IOException
    {
        lineReader.close();
    }

    @Override
    public Text createKey()
    {
        return new Text("");
    }

    @Override
    public Text createValue()
    {
        return new Text("");
    }

    @Override
    public long getPos() throws IOException
    {
        return lineReader.getPos();
    }

    @Override
    public float getProgress() throws IOException
    {
        return lineReader.getProgress();
    }

    @Override
    public boolean next(Text key, Text value) throws IOException
    {
        System.out.println("Next Key: " + nextKey.toString());
        /* If we don't have another key laying around */
        if (nextKey.charAt(0) != '>')
        {
            // get the next line
            if (!lineReader.next(lineKey, lineValue)) {
                return false;
            }
        
            /* Our key in Fasta is a line that starts with a '>'. 
             * Skip lines until we get to a line that starts with '>' 
             */
            while(lineValue.charAt(0) != '>')
            {
                if(lineReader.next(lineKey, lineValue) == false)
                    return false;
            }
        
            /* We have our key. Save it off */
            key.set(lineValue);
        }
        else
        {
            key.set(nextKey);
        }
        System.out.println("Key: " + key.toString());
        
        /* Now read the values (sequence data). There can be multiple 
         * lines of sequence data. Accumulate all lines until we reach a 
         * line that starts with a '>' which indicates the start of the 
         * next read
         */        
        /* Get the next line */
        value.clear();
        if(lineReader.next(lineKey, lineValue) == false)
        {
            System.out.println("False lineValue: " + lineValue.toString());
            return false;
        }
        nextKey.set(lineValue);
        
        System.out.println("First lineValue: " + lineValue.toString());
        while(nextKey.charAt(0) != '>')
        {
            value.append(nextKey.getBytes(), 0, nextKey.getLength());
            System.out.println("Looop value: " + value.toString());
            /* We have hit the end */
            if(lineReader.next(lineKey, lineValue) == false)
                break;
            nextKey.set(lineValue);
            System.out.println("Loop lineValue: " + lineValue.toString());
        }
        
        /* We have successfully read if lineValue is greater than zero */
        return (lineValue.getLength() > 0);
    }
}
