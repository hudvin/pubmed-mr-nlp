package com.nevilon.pubmednlp.core.utils;

import org.apache.commons.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: hudvin
 * Date: 9/7/13
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class SequenceFileCreator {


    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String args[]) throws Exception{

        String uri = "/tmp/input/sq";
        Configuration conf = new Configuration();
        FileSystem fssq = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Writer writer = null;


        try{

            writer = SequenceFile.createWriter(fssq, conf, path,
                    key.getClass(), value.getClass());



            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] statuses = fs.listStatus(new Path("/media/data/pdfs"));  // you need to pass in your hdfs path
            for (int i=0;i<100;i++){
                FileStatus status= statuses[i];
                if(status.getPath().getName().endsWith(".pdf")){
                    System.out.println(i);
                    byte[]data = org.apache.commons.io.IOUtils.toByteArray(new BufferedReader(new InputStreamReader(fs.open(status.getPath())),1024*64));
                    key.set(String.valueOf(System.currentTimeMillis()));
                    value.set(data,0,data.length);
                    writer.append(key, value);
                }
            }
        }catch(Exception e){
            System.out.println("File not found");
        }

        IOUtils.closeStream(writer);

}

}
