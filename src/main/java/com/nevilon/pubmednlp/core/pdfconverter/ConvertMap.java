package com.nevilon.pubmednlp.core.pdfconverter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ConvertMap extends Mapper<NullWritable, BytesWritable, PdfFile, NullWritable> {

    private String filename;

    private MultipleOutputs mOutput;

    private String path;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path absPath =  ((FileSplit) context.getInputSplit()).getPath();
        this.path = absPath.toString();
        this.filename = absPath.getName();
        mOutput = new MultipleOutputs(context);
    }


    public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String text = parse(value.getBytes());
        context.write(new PdfFile(filename, text, path), NullWritable.get());
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mOutput.close();
    }

    private String parse(byte[] data) {
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
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return text;
    }

}
