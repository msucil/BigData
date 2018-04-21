package com.msucil.hadoop.wordcount.v1;

import com.msucil.hadoop.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Created by msucil on 4/21/18.
 */
public class WordCountTest {

    @Test
    public void processWordCount() throws IOException {
        MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
                MapReduceDriver.newMapReduceDriver(new WordCountMapper(), new WordCountReducer());

        Text input = new Text("Hello BigData Hadoop");

        mapReduceDriver.withInput(new LongWritable(0), input)
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("BigData"), new IntWritable(1)),
                        new Pair<>(new Text("Hadoop"), new IntWritable(1)),
                        new Pair<>(new Text("Hello"), new IntWritable(1)))
                )
                .runTest();
    }

    @Test
    public void processWordCountOnLocalData() throws Exception {
        final Configuration config = new Configuration();

        config.set("fs.defaultFS", "local");
        config.set("mapreduce.framework.name", "local");
        config.setInt("mapreduce.task.io.sort.mb", 1);

        final Path input = new Path("input/wordcount/wordcount.txt");
        final Path outout = new Path("output");

        final FileSystem fs = FileSystem.getLocal(config);
        fs.delete(outout, true);

        final WordCount wordCount = new WordCount();
        wordCount.setConf(config);

        int exitCode = wordCount.run(new String[]{input.toString(), outout.toString()});

        assertEquals(exitCode, 0);
    }
}
