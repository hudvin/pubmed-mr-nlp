package com.nevilon.pubmednlp.core;


import com.nevilon.pubmednlp.core.utils.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Runner {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);
        job.setJarByClass(Runner.class);

        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        SequenceFileAsBinaryInputFormat.addInputPath(job, new Path("/media/data/seq/"));


        job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class
        );

        SequenceFileAsBinaryInputFormat.addInputPath(job, new Path("/media/data/seq/"));


        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/media/data/seq/"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/anotherffe"));
        //wait
        job.waitForCompletion(true);


    }

}
