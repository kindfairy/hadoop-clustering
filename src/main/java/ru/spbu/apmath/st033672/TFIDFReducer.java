package ru.spbu.apmath.st033672;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class TFIDFReducer extends Reducer<Text, StringDouble, Text, StringDouble> {

    private int docNum = 18828;

    @Override
    public void reduce(Text word, Iterable<StringDouble> values, Context context)
            throws IOException, InterruptedException {

        /*
        for( StringDouble value: values ){
            context.write(word, value);
        }
        */


        List<StringDouble> list = new ArrayList<>();
        int df = 0;
        Iterator<StringDouble> iterator = values.iterator();
        while (iterator.hasNext()) {
            list.add(new StringDouble(iterator.next()));
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
            StringDouble docNameDouble = iterator.next();
            double oldValue = docNameDouble.getValue();
            double newValue = oldValue * Math.log(((double) docNum / (double) df));
            //double newValue = Math.log(((double) docNum / (double) df));

            docNameDouble.setValue(newValue);
            context.write(word, docNameDouble);
        }


    }
}