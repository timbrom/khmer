package edu.msu.cse.diginorm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Diginorm extends Configured implements Tool
{
    public static void main(String[] args) throws Exception 
    {        
        int rc = ToolRunner.run(new Configuration(), new Diginorm(), args);
        System.exit(rc);
    }

    public int run(String[] args) throws Exception 
    {
        if (args.length != 2)
        {
            System.err.println("Usage: Diginorm <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(Diginorm.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setInputFormatClass(FastaInputFormat.class);

        job.setMapperClass(DiginormMapper.class);
        job.setReducerClass(DiginormReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return(job.waitForCompletion(true) ? 0 : 1);
    }
}
