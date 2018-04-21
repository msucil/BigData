package com.msucil.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    @Test
    public void returnsTotalCountForMultipleWords() throws IOException {

        final Pair<Text, List<IntWritable>> input1 = new Pair<>(new Text("Hadoop"),
                Arrays.asList(new IntWritable(2), new IntWritable(1)));
        final Pair<Text, List<IntWritable>> input2 = new Pair<>(new Text("BigData"),
                Arrays.asList(new IntWritable(1)));

        final List<Pair<Text, List<IntWritable>>> inputs = new ArrayList<>();
        inputs.add(input1);
        inputs.add(input2);

        final Pair<Text, IntWritable> output1 = new Pair<>(new Text("Hadoop"), new IntWritable(3));
        final Pair<Text, IntWritable> output2 = new Pair<>(new Text("BigData"), new IntWritable(1));

        final List<Pair<Text, IntWritable>> outputs = new ArrayList<>();
        outputs.add(output1);
        outputs.add(output2);

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new WordCountReducer())
                .withAll(inputs)
                .withAllOutput(outputs)
                .runTest();
    }
}
