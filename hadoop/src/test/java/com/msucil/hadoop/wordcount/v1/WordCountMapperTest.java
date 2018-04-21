package com.msucil.hadoop.wordcount.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by msucil on 4/21/18.
 */
public class WordCountMapperTest {

    @Test
    public void returnsWordCountInText() throws IOException {
        final Text value = new Text("Hello Hadoop\n Hadoop");
        final List<Pair<Text, IntWritable>> mapResult = new ArrayList<>();

        final Pair<Text, IntWritable> word1 = new Pair<>(new Text("Hello"), new IntWritable(1));
        final Pair<Text, IntWritable> word2 = new Pair<>(new Text("Hadoop"), new IntWritable(1));

        mapResult.add(word1);
        mapResult.add(word2);
        mapResult.add(word2);

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), value)
                .withAllOutput(mapResult)
                .runTest();
    }

    @Test
    public void returnsNothingOnBlankText() throws IOException {
        final List<Pair<Text, IntWritable>> mapResult = new ArrayList<>();

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), new Text(""))
                .withAllOutput(mapResult)
                .runTest();
    }
}
