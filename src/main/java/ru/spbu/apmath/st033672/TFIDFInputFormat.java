package ru.spbu.apmath.st033672;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class TFIDFInputFormat extends InputFormat<Text, StringDouble> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

        List<InputSplit> list = new ArrayList<>();

        //по всем input path (на самом деле он один)
        for (Path path : FileInputFormat.getInputPaths(context)) {

            FileSystem fs = path.getFileSystem(context.getConfiguration());
            //по всем файлам в директориях
            for (FileStatus file : fs.listStatus(path)) {
                //1 файл - 1 inputsplit

                list.add(new CombineFileSplit(new Path[]{file.getPath()}, new long[]{file.getLen()}));

            }
        }

        return list;
    }


    @Override
    public RecordReader<Text, StringDouble> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        return new TFIDFRecordReader();

    }

}
