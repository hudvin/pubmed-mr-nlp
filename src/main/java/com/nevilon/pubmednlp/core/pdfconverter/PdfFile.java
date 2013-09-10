package com.nevilon.pubmednlp.core.pdfconverter;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: hudvin
 * Date: 9/8/13
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class PdfFile implements WritableComparable<PdfFile>{
    @Override
    public int compareTo(PdfFile o) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
