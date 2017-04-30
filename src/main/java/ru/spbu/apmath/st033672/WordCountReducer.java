package ru.spbu.apmath.st033672;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;


public class WordCountReducer extends Reducer<DocNameWord, IntWritable, DocNameWord, IntWritable>
{
    @Override
    public void reduce(DocNameWord key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {


        int count = 0;

        for( IntWritable value: values ){
            count += value.get();
        }

        IntWritable result = new IntWritable(count);

        context.write(key, result);

    }
}