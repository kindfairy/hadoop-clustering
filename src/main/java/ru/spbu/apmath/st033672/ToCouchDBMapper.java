package ru.spbu.apmath.st033672;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;


public class ToCouchDBMapper extends Mapper<Text, StringDouble, Text, StringDouble> {

    public void map(Text docName, StringDouble stringDouble, Context context)
            throws IOException, InterruptedException {

        context.write(docName, stringDouble);

    }

}
