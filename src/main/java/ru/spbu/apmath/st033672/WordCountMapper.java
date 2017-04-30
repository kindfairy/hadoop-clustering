package ru.spbu.apmath.st033672;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.*;

import org.apache.lucene.analysis.en.PorterStemFilter;

import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.*;


import java.io.*;
import java.util.*;


public class WordCountMapper extends Mapper<Text, Text, DocNameWord, IntWritable> {

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        //abstract
        Reader reader = new StringReader(value.toString());
        Analyzer analyzer = new StandardAnalyzer();

        //with PorterStemmer
        TokenStream stream = new PorterStemFilter(analyzer.tokenStream(null, reader));

        //without PorterStemmer
        //TokenStream stream = analyzer.tokenStream(null, reader);



        stream.reset();
        while (stream.incrementToken()) {
            String token = stream
                    .getAttribute(CharTermAttribute.class)
                    .toString();
            context.write(new DocNameWord(key.toString(), token), new IntWritable(1));
        }
        stream.end();
        stream.close();


    }

}
