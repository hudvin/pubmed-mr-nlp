package com.nevilon.pubmednlp.core.reverb;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtractMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private ReVerbExtractor reverb = new ReVerbExtractor();
    private OpenNlpSentenceChunker chunker;
    private ConfidenceFunction confFunc;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        System.out.println(filename);
        chunker = new OpenNlpSentenceChunker();
        confFunc = new ReVerbOpenNlpConfFunction();
    }

    private List<Relation> extract(String text) throws Exception {
        System.out.println("extracting...");
        ChunkedSentence sent = chunker.chunkSentence(text);
        List<Relation> rels = new ArrayList<Relation>();
        for (ChunkedBinaryExtraction extr : reverb.extract(sent)) {
            double conf = confFunc.getConf(extr);
            String arg1 = extr.getArgument1().toString();
            String rel = extr.getRelation().toString();
            String arg2 = extr.getArgument2().toString();
            //String sentence = extr.getSentence().toString();
            Relation relData = new Relation();
            relData.setArg1(arg1);
            relData.setArg2(arg2);
            relData.setRel(rel);
            relData.setConfidence(conf);
            //relData.setSentence(sentence);
            //fill json
            rels.add(relData);
        }
        return rels;
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> data = new Gson().fromJson(value.toString(), Map.class);
        String filename = data.get("filename");
        String text = data.get("text");
        String path = data.get("path");
        Map<String, Object> outputData = new HashMap<String, Object>();
        outputData.put("text", text);
        outputData.put("filename", filename);
        outputData.put("path", path);
        try {
            List<Relation> rels = extract(text);
            outputData.put("relations", rels);
            Gson gson = new Gson();
            String output = gson.toJson(outputData);
            context.write(new Text(output), NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

