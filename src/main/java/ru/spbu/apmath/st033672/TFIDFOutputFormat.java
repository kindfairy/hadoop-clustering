package ru.spbu.apmath.st033672;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class TFIDFOutputFormat extends FileOutputFormat<Text, Text>{

    @Override
    public  RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context){
        //Get the RecordWriter for the given task.

        return new TFIDFRecordWriter();

    }



}