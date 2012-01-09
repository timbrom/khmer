package edu.msu.cse.diginorm;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

public class FastaRecordReader implements RecordReader<Text, Text>
{
    private long       start;
    private long       pos;
    private long       end;
    private LineReader in;
    int                maxLineLength;
    private Text       nextKey;
    boolean lastKey = false;

    public FastaRecordReader(JobConf job, FileSplit input) throws IOException
    {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        start = input.getStart();
        end = start + input.getLength();
        final Path file = input.getPath();

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(input.getPath());
        fileIn.seek(start);
        in = new LineReader(fileIn, job);

        this.pos = start;
        nextKey = new Text(" ");
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

    public float getProgress()
    {
        if (start == end)
        {
            return 0.0f;
        } else
        {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized long getPos() throws IOException
    {
        return pos;
    }

    public synchronized void close() throws IOException
    {
        if (in != null)
        {
            in.close();
        }
    }

    public synchronized boolean next(Text key, Text value) throws IOException
    {
        if (pos > end)
            return false;

        /* If we don't have another key laying around */
        if (nextKey.charAt(0) != '>')
        {
            /* get the next line */
            int newSize = in.readLine(nextKey, maxLineLength);
            if (newSize == 0)
            {
                return false;
            }
            pos += newSize;
            /*
             * Our key in Fasta is a line that starts with a '>'. Skip lines
             * until we get to a line that starts with '>'
             */
            while (nextKey.charAt(0) != '>')
            {
                newSize = in.readLine(nextKey, maxLineLength);
                if (newSize == 0)
                {
                    return false;
                }
                pos += newSize;
            }

            /* We have our key. Save it off */
            key.set(nextKey);
        } else
        {
            key.set(nextKey);
        }

        /*
         * Now read the values (sequence data). There can be multiple lines of
         * sequence data. Accumulate all lines until we reach a line that starts
         * with a '>' which indicates the start of the next read
         */
        /* Get the next line */
        value.clear();
        int newSize = in.readLine(nextKey, maxLineLength);
        if (newSize == 0)
        {
            return false;
        }
        pos += newSize;

        while (nextKey.charAt(0) != '>')
        {
            value.append(nextKey.getBytes(), 0, nextKey.getLength());
            /* We have hit the end */
            newSize = in.readLine(nextKey, maxLineLength);
            if (newSize == 0) // EOF
            {
                nextKey.set(" ");
                break;
            }
            pos += newSize;
        }
        
        /* If I have read a key into the nextKey variable, this is 
         * the key I need to use the next time this function is called.
         * I also need to make sure that this function returns the next
         * key/value pair the next time it is called, so set the end to 
         * greater than the current position.
         */
        if (pos > end && lastKey == false)
        {
            lastKey = true;
            end = pos + 1;
        }

        /* We have successfully read if value is greater than zero */
        return (value.getLength() > 0);
    }
}
