package ru.spbu.apmath.st033672;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.google.gson.*;

import java.io.IOException;
import java.util.*;


public class TFIDFRecordWriter extends RecordWriter<Text, Text> {

    private Gson gson = new Gson();


    //hardcode :(((
    private static String ip = "217.197.2.6";
    private static String port = "5984";
    private static String dbName = "tfidf";
    private static String userName = "admin";
    private static String userPassword = "admin";

    private static final int MAX_SIZE = 1000;

    List<String> list = new ArrayList<>(MAX_SIZE);


    public void write(Text key, Text value) {

        list.add(value.toString());

        if (list.size() >= MAX_SIZE) {
            //TODO
            //create CouchDBConnector
            CouchdbConnector connector = new CouchdbConnector(ip, port, dbName, userName, userPassword);
            //write bulk
            try {
                connector.writeBulkJsons(list);
            } catch (IOException e) {
                e.printStackTrace();
            }

            list = new ArrayList<>(MAX_SIZE);
        }

    }

    public void close(TaskAttemptContext context) {

        //TODO
        //create CouchDBConnector
        CouchdbConnector connector = new CouchdbConnector(ip, port, dbName, userName, userPassword);

        try {
            connector.writeBulkJsons(list);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //write Remaining


    }

}