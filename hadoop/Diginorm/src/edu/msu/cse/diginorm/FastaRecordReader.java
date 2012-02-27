package edu.msu.cse.diginorm;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class FastaRecordReader extends RecordReader<LongWritable, Text>
{
    private long       start;
    private long       pos;
    private long       end;
    private LineReader in;
    int                maxLineLength;
    boolean lastKey = false;
    
    private Text tmpText;
    private Text lineValue;
    private LongWritable linePos;
    private LongWritable nextLinePos;

    public FastaRecordReader() throws IOException, InterruptedException
    {
        lineValue = new Text();
        tmpText = new Text(" ");
        linePos = new LongWritable();
        nextLinePos = new LongWritable();
    }

    @Override
    public void close() throws IOException
    {
        if (in != null)
        {
            in.close();
        }
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
        if (start == end)
        {
            return 0.0f;
        } else
        {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void initialize(InputSplit input, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        this.maxLineLength = context.getConfiguration().getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        FileSplit split = (FileSplit) input;
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(split.getPath());
        fileIn.seek(start);
        in = new LineReader(fileIn, context.getConfiguration());
        
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {       
        // If we hit the end of our split
        if (pos > end)
            return false;
        
        /* Our key in Fasta is a line that starts with a '>'.
         * The key itself is the byte position of the '>' character 
         * Skip lines until we get to a line that starts with '>' 
         */
        
        while(tmpText.charAt(0) != '>')
        {
            int newSize = in.readLine(tmpText, maxLineLength);
            if (newSize == 0)
            {
                return false;
            }
            nextLinePos.set(pos);
            pos += newSize;
        }
        
        /* We have our key. Save it off */
        linePos.set(nextLinePos.get());

        
        /* Now read the values (sequence data). There can be multiple 
         * lines of sequence data. Accumulate all lines until we reach a 
         * line that starts with a '>' which indicates the start of the 
         * next read
         */
        lineValue.clear();
        int newSize = in.readLine(tmpText, maxLineLength);
        if (newSize == 0)
        {
            return false;
        }
        nextLinePos.set(pos);
        pos += newSize;
        
        while(tmpText.charAt(0) != '>')
        {
            lineValue.append(tmpText.getBytes(), 0, tmpText.getLength());
            /* We have hit the end */
            newSize = in.readLine(tmpText, maxLineLength);
            if (newSize == 0) // EOF
            {
                tmpText.set(" ");
                break;
            }
            nextLinePos.set(pos);
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
        
        /* We have successfully read if lineValue is greater than zero */
        return (lineValue.getLength() > 0);
    }
}
