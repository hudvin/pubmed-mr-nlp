package com.nevilon.pubmednlp.core.utils;

import edu.washington.cs.knowitall.extractor.ReVerbExtractor;
import edu.washington.cs.knowitall.extractor.conf.ConfidenceFunction;
import edu.washington.cs.knowitall.extractor.conf.ReVerbOpenNlpConfFunction;
import edu.washington.cs.knowitall.nlp.ChunkedSentence;
import edu.washington.cs.knowitall.nlp.OpenNlpSentenceChunker;
import edu.washington.cs.knowitall.nlp.extraction.ChunkedBinaryExtraction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tika.exception.TikaException;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.codehaus.jettison.json.JSONArray;
import org.xml.sax.SAXException;

import java.io.*;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

//Mapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public  class Map extends Mapper<BytesWritable, BytesWritable, NullWritable, Text>
    {

        private  OpenNlpSentenceChunker chunker;
        private ConfidenceFunction confFunc;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            System.out.println("creating");
            super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
            chunker = new OpenNlpSentenceChunker();
            confFunc = new ReVerbOpenNlpConfFunction();
        }

        private ReVerbExtractor reverb = new ReVerbExtractor();



        private List<String> extract(String text) throws  Exception{
            System.out.println("extracting... 1");
            ChunkedSentence sent = chunker.chunkSentence(text);
            System.out.println("extracting... 2");

            List<String> rels = new ArrayList<String>();
            for (ChunkedBinaryExtraction extr : reverb.extract(sent)) {
                double conf = confFunc.getConf(extr);
                String arg1 = extr.getArgument1().toString();
                String rel = extr.getRelation().toString();
                String arg2 = extr.getArgument2().toString();
                //fill json
                rels.add(arg1+ " ==== " + rel + " ==== " + arg2);
            }
            return rels;
        }

        private String parse(byte[] data){
            InputStream is = null;
            String text = "";

            try {

                is = new ByteArrayInputStream(data);

                ContentHandler handler = new BodyContentHandler(Integer.MAX_VALUE);
                Metadata metadata = new Metadata();
                new PDFParser().parse(is, handler, metadata, new ParseContext());
                text = handler.toString();

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return text;
        }

        //map(KEYIN key, VALUEIN value, org.apache.hadoop.mapreduce.Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>.Context context)
        public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException
        {

            String text = parse(value.getBytes());
            try {
                List<String> rels = extract(text);
                for (String rel : rels){
                    context.write(NullWritable.get(), new Text(rel));
                }
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }
