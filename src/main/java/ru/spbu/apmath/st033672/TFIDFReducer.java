package ru.spbu.apmath.st033672;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class TFIDFReducer extends Reducer<Text, DocNameDouble, Text, DocNameDouble> {

    private int docNum = 18828;

    @Override
    public void reduce(Text word, Iterable<DocNameDouble> values, Context context)
            throws IOException, InterruptedException {

        /*
        for( DocNameDouble value: values ){
            context.write(word, value);
        }
        */


        List<DocNameDouble> list = new ArrayList<>();
        int df = 0;
        Iterator<DocNameDouble> iterator = values.iterator();
        while (iterator.hasNext()) {
            list.add(new DocNameDouble(iterator.next()));
            df++;
        }


        /*
        iterator = list.iterator();
        while (iterator.hasNext()){
            context.write(word, iterator.next());
        }
        */


        iterator = list.iterator();

        while (iterator.hasNext()) {
            DocNameDouble docNameDouble = iterator.next();
            double oldValue = docNameDouble.getValue();
            double newValue = oldValue * Math.log(((double) docNum / (double) df));
            //double newValue = Math.log(((double) docNum / (double) df));

            docNameDouble.setValue(newValue);
            context.write(word, docNameDouble);
        }


    }
}