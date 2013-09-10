package com.nevilon.pubmednlp.core.pdfconverter;

import com.nevilon.pubmednlp.core.utils.WholeFileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PdfConverterJob  extends Configured
        implements Tool {

    public int run(String[] args)  throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


//        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
//        if (job == null) {
//            return -1;
//        }

        job.setJarByClass(PdfConverterJob.class);

        job.setOutputKeyClass(Text.class)  ;
        job.setOutputValueClass(Text.class) ;

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(ConvertMap.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PdfConverterJob(), args);
        System.exit(exitCode);
    }


}
