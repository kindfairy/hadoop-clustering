package ru.spbu.apmath.st033672;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by henry on 4/30/17.
 */
public class DocNameWord implements WritableComparable<DocNameWord> {


    private String docName;
    private String word;

    public DocNameWord(){}

    public DocNameWord(String docName, String word) {
        this.docName = docName;
        this.word = word;
    }

    public String getDocName() {
        return docName;
    }

    public String getWord() {
        return word;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(docName);
        dataOutput.writeUTF(word);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docName = dataInput.readUTF();
        word = dataInput.readUTF();
    }

    @Override
    public String toString(){
        return docName + "\t" + word;
    }

    @Override
    public int compareTo(DocNameWord o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public int hashCode(){
        return 0;
    }
}
