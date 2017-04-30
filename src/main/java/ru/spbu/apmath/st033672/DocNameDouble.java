package ru.spbu.apmath.st033672;

import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by henry on 4/30/17.
 */
public class DocNameDouble implements Writable {


    private String docName;
    private double value;

    public DocNameDouble() {
    }

    public DocNameDouble(String docName, double value) {
        this.docName = docName;
        this.value = value;
    }

    public DocNameDouble(DocNameDouble that){
        this.docName = that.docName;
        this.value = that.value;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(docName);
        dataOutput.writeDouble(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docName = dataInput.readUTF();
        value = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return docName + "\t" + value;
    }
}
