package ru.spbu.apmath.st033672;

import com.google.gson.Gson;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class ToCouchDBReducer extends Reducer<Text, StringDouble, Text, Text> {

    private int docNum = 18828;

    @Override
    public void reduce(Text docName, Iterable<StringDouble> values, Context context)
            throws IOException, InterruptedException {

        List<StringDouble> stringDoubles = new ArrayList<>();

        for( StringDouble value: values ){
            stringDoubles.add(new StringDouble(value));
        }

        DocTFIDF docTFIDF = new DocTFIDF(docName.toString(), stringDoubles);

        Gson gson = new Gson();

        String json = gson.toJson(docTFIDF);

        context.write(new Text(), new Text(json));

    }
}