package com.msucil.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by msucil on 4/21/18.
 */
public class WordCountReducerTest {

    @Test
    public void returnsTotalNoOfWords() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new WordCountReducer())
                .withInput(new Text("hadoop"), Arrays.asList(
                        new IntWritable(2), new IntWritable(3), new IntWritable(1)
                ))
                .withOutput(new Text("hadoop"), new IntWritable(6))
                .runTest();
    }
}
