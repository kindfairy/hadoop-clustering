package ru.spbu.apmath.st033672;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by henry on 4/29/17.
 */
public class TFIDFRecordReader extends RecordReader<Text, StringDouble> {

    private Text docName;
    private StringDouble stringDouble;

    private Queue<Text> docNames = new LinkedList<>();
    private Queue<StringDouble> stringDoubles = new LinkedList<>();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit;

        Path[] paths = combineFileSplit.getPaths();

        for (Path path : paths) {

            FileSystem fs = path.getFileSystem(taskAttemptContext.getConfiguration());
            FSDataInputStream inputStream = fs.open(path);
            LineReader reader = new LineReader(inputStream);

            Text tmp = new Text();
            while (reader.readLine(tmp) != 0) {

                String[] strings = tmp.toString().split("\t");
                docNames.offer(new Text(strings[1]));
                stringDoubles.offer(new StringDouble(strings[0], Double.parseDouble(strings[2])));

            }

            reader.close();

        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (docNames.isEmpty()) return false;

        docName = docNames.poll();
        stringDouble = stringDoubles.poll();

        return true;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return docName;
    }

    @Override
    public StringDouble getCurrentValue() throws IOException, InterruptedException {
        return stringDouble;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
