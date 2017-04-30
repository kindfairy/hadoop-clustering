package ru.spbu.apmath.st033672;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.lucene.analysis.*;

import org.apache.lucene.analysis.en.PorterStemFilter;

import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.*;


import java.io.*;
import java.util.*;


public class TFIDFMapper extends Mapper<Text, Text, Text, StringDouble> {

    public void map(Text docName, Text docText, Context context)
            throws IOException, InterruptedException {

        //abstract
        Reader reader = new StringReader(docText.toString());
        Analyzer analyzer = new StandardAnalyzer();

        //with PorterStemmer
        TokenStream stream = new PorterStemFilter(analyzer.tokenStream(null, reader));

        //without PorterStemmer
        //TokenStream stream = analyzer.tokenStream(null, reader);

        Map<String, Double> map = new HashMap<>();

        double total = 0.0;

        stream.reset();
        while (stream.incrementToken()) {
            String token = stream
                    .getAttribute(CharTermAttribute.class)
                    .toString();
            if (map.containsKey(token)) {
                map.put(token, map.get(token) + 1.0);
            } else {
                map.put(token, 1.0);
            }
            total++;
        }

        for (Map.Entry<String, Double> entry : map.entrySet()) {
            context.write(new Text(entry.getKey()),
                    new StringDouble(docName.toString(), entry.getValue() / total));
        }

        stream.end();
        stream.close();


    }

}
