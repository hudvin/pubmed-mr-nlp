package com.nevilon.pubmednlp.core.pdfconverter;

import com.google.common.collect.ComparisonChain;
import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PdfFile implements WritableComparable<PdfFile> {

    private String filename;
    private String text;
    private String path;

    public PdfFile() {
    }

    public PdfFile(String filename, String text, String path) {
        this.filename = filename;
        this.text = text;
        this.path = path;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        filename = in.readUTF();
        text = in.readUTF();
        path = in.readUTF();
    }

    @Override
    public String toString() {
        Map<String, String> gsonData = new HashMap<String, String>();
        gsonData.put("filename", this.filename);
        gsonData.put("path", this.path);
        gsonData.put("text", this.text);
        Gson gson = new Gson();
        return gson.toJson(gsonData);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(filename);
        out.writeUTF(text);
        out.writeUTF(path);
    }

    @Override
    public int compareTo(PdfFile o) {
        return ComparisonChain.start().
                compare(filename, o.filename).
                compare(path, o.path).
                compare(text, o.text).result();
    }

}