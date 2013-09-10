package com.nevilon.pubmednlp.core.reverb;


//import edu.washington.cs.knowitall.extractor.ReVerbExtractor;
//import edu.washington.cs.knowitall.extractor.conf.ConfidenceFunction;
//import edu.washington.cs.knowitall.nlp.OpenNlpSentenceChunker;
import com.google.gson.Gson;
import edu.washington.cs.knowitall.extractor.ReVerbExtractor;
import edu.washington.cs.knowitall.extractor.conf.ConfidenceFunction;
import edu.washington.cs.knowitall.extractor.conf.ReVerbOpenNlpConfFunction;
import edu.washington.cs.knowitall.nlp.ChunkedSentence;
import edu.washington.cs.knowitall.nlp.OpenNlpSentenceChunker;
import edu.washington.cs.knowitall.nlp.extraction.ChunkedBinaryExtraction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;

import java.io.IOException;
import java.util.*;

public class ExtractMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ReVerbExtractor reverb =  new ReVerbExtractor();
    private OpenNlpSentenceChunker chunker;
    private ConfidenceFunction confFunc;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      //  System.out.println("creating");
      //  super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        System.out.println(filename);
        chunker = new OpenNlpSentenceChunker();
        confFunc = new ReVerbOpenNlpConfFunction();
    }



    static class Relation{

        private String arg1;

        String getArg1() {
            return arg1;
        }

        void setArg1(String arg1) {
            this.arg1 = arg1;
        }

        String getArg2() {
            return arg2;
        }

        void setArg2(String arg2) {
            this.arg2 = arg2;
        }

        String getRel() {
            return rel;
        }

        void setRel(String rel) {
            this.rel = rel;
        }

        private String arg2;

        private String rel;

        String getSentence() {
            return sentence;
        }

        void setSentence(String sentence) {
            this.sentence = sentence;
        }

        private String sentence;

    }

    private List<Relation> extract(String text) throws Exception {
        System.out.println("extracting... 1");
        ChunkedSentence sent = chunker.chunkSentence(text);
        System.out.println("extracting... 2");
        List<Relation> rels = new ArrayList<Relation>();
        for (ChunkedBinaryExtraction extr : reverb.extract(sent)) {
            double conf = confFunc.getConf(extr);
            String arg1 = extr.getArgument1().toString();
            String rel = extr.getRelation().toString();
            String arg2 = extr.getArgument2().toString();
            String sentence = extr.getSentence().toString();
            Relation relData = new Relation();
            relData.setArg1(arg1);
            relData.setArg2(arg2);
            relData.setRel(rel);
            relData.setSentence(sentence);
            //fill json
            rels.add(relData);
        }
        return rels;
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String,String> data = new Gson().fromJson(value.toString(), Map.class);
        String filename = data.get("filename");
        String text = data.get("text");
        String path = data.get("path");
        Map<String, Object> outputData = new HashMap<String, Object>();
        outputData.put("text", text);
        outputData.put("filename", filename);
        outputData.put("path",path);
        try {
            List<Relation> rels = extract(text);
            outputData.put("relations", rels);

            Gson gson = new Gson();
            String output =  gson.toJson(outputData);
            context.write(new Text(output),new Text(""));
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
