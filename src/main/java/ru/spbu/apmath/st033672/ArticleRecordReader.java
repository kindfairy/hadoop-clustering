package ru.spbu.apmath.st033672;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by henry on 4/29/17.
 */
public class ArticleRecordReader extends RecordReader<Text, Text> {

    private Text fileName;
    private Text text;

    private static class Article{
        private Text filename;
        private Text text;

        public Article(Text filename, Text text) {
            this.filename = filename;
            this.text = text;
        }

        public Text getFilename() {
            return filename;
        }

        public Text getText() {
            return text;
        }
    }

    private Queue<Article> queue = new LinkedList<>();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit;

        Path[] paths = combineFileSplit.getPaths();

        for( Path path: paths ){

            FileSystem fs = path.getFileSystem(taskAttemptContext.getConfiguration());
            FSDataInputStream inputStream = fs.open(path);
            LineReader reader = new LineReader(inputStream);

            StringBuilder sb = new StringBuilder();
            Text tmp = new Text();
            while (reader.readLine(tmp)!= 0){
                sb.append(tmp.toString());
            }
            queue.offer(new Article(new Text(path.getName()), new Text(sb.toString())));
            reader.close();

        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if( queue.isEmpty() ) return false;

        Article article = queue.poll();

        fileName = article.getFilename();
        text = article.getText();
        return  true;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return fileName;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return text;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
