package edu.msu.cse.diginorm; 
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Diginorm extends Configured implements Tool
{
    public static void main(String[] args) throws Exception 
    {        
        Configuration configuration = new Configuration();
        int rc = ToolRunner.run(configuration, new Diginorm(), args);
        System.exit(rc);
    }

    public int run(String[] args) throws Exception 
    {
        if (args.length != 2)
        {
            System.err.println("Usage: Diginorm <input path> <output path>");
            System.exit(-1);
        }
        
        // Configuration processed by ToolRunner
        Configuration conf = getConf(); //new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(Diginorm.class);
        
        // Temporary directory for intermediate output
        Path tmpDir = new Path("diginorm-tmp-" + new Random().nextInt());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tmpDir);
        
        job.setInputFormatClass(FastaInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setMapperClass(MapSeqToKmers.class);
        job.setReducerClass(ReduceCountKmers.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        if(job.waitForCompletion(true))
        {
            // Configuration processed by ToolRunner
            Configuration conf2 = getConf(); //new Configuration();
            Job job2 = new Job(conf2);
            job2.setJarByClass(Diginorm.class);

            FileInputFormat.addInputPath(job2, tmpDir);
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            
            job2.setMapperClass(IdentityMapper.class);
            job2.setReducerClass(MedianCountReducer.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(NullWritable.class);
            
            int rc = (job2.waitForCompletion(true) ? 0 : 1);
            FileSystem.get(conf2).delete(tmpDir, true);
            return rc;
        }
        int rc = (job.waitForCompletion(true) ? 0 : 1);
        FileSystem.get(conf).delete(tmpDir, true);
        return rc;
    }
}
