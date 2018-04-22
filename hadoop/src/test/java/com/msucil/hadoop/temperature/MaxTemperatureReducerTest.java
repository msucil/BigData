package com.msucil.hadoop.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by msucil on 4/20/18.
 */
public class MaxTemperatureReducerTest {

    @Test
    public void returnsMaxTemperatureForYear() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1990"), Arrays.asList(new IntWritable(100), new IntWritable(-20),
                        new IntWritable(500)))
                .withOutput(new Text("1990"), new IntWritable(500))
                .runTest();
    }
}
