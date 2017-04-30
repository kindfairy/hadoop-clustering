package ru.spbu.apmath.st033672;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.util.*;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {


		//0 docs
		//1 after tokenization
        //2 after calculations document frequency
        if (args.length != 3) {
			ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir> <output-dir>");
            System.exit(1);
        }

		/*
		Job job = Job.getInstance(getConf());
        job.setJarByClass(Main.class);
		job.setJobName("WordCounter");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(Summer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(WordFrequencyOutputFormat.class);

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

        return job.waitForCompletion(true) ? 0 : 1;

		*/




        //step 1   tf
        //filename - text -> [ filename word n/N ]
        Job tfidfJob = Job.getInstance(getConf());
        tfidfJob.setJarByClass(Main.class);
        tfidfJob.setJobName("TFIDF");
        FileInputFormat.addInputPath(tfidfJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(tfidfJob, new Path(args[1]));

        //tfidfJob.setInputFormatClass(FileInputFormat.class);
        tfidfJob.setInputFormatClass(ArticleInputFormat.class);

		tfidfJob.setMapperClass(TFIDFMapper.class);
		tfidfJob.setMapOutputKeyClass(Text.class);
		tfidfJob.setMapOutputValueClass(StringDouble.class);

		tfidfJob.setReducerClass(TFIDFReducer.class);

        //No custom OutpitFormat

        if ( !tfidfJob.waitForCompletion(true) ) return 1;




        //step 2 reduceByDoc, Write to Couchdb
        Job toCouchDBJob = Job.getInstance(getConf());
        toCouchDBJob.setJarByClass(Main.class);
        toCouchDBJob.setJobName("toCouchDB");
        FileInputFormat.addInputPath(toCouchDBJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(toCouchDBJob, new Path(args[2]));

        //TODO
        toCouchDBJob.setInputFormatClass(TFIDFInputFormat.class);

        //TODO
        toCouchDBJob.setMapperClass(ToCouchDBMapper.class);
        toCouchDBJob.setMapOutputKeyClass(Text.class);
        toCouchDBJob.setMapOutputValueClass(StringDouble.class);

        toCouchDBJob.setReducerClass(ToCouchDBReducer.class);

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(tfidfJob)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(tfidfJob));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(toCouchDBJob));

        return toCouchDBJob.waitForCompletion(true) ? 0 : 1;

		


    }

}
