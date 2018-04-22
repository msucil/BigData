package com.msucil.hadoop.wordcount.v2;

import org.apache.hadoop.conf.Configuration;
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
 * Created by msucil on 4/22/18.
 */
public class WordCountMapperTest {

    @Test
    public void returnsWordCountWithIgnoreCase() throws IOException {
        final Text value = new Text("Hello Hadoop\n Hadoop");
        final List<Pair<Text, IntWritable>> mapResult = new ArrayList<>();

        final Pair<Text, IntWritable> word1 = new Pair<>(new Text("hello"), new IntWritable(1));
        final Pair<Text, IntWritable> word2 = new Pair<>(new Text("hadoop"), new IntWritable(1));

        mapResult.add(word1);
        mapResult.add(word2);
        mapResult.add(word2);

        Configuration config = new Configuration();
        config.set("wordCount.ignoreCase", "true");

        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), value)
                .withAllOutput(mapResult);

        mapDriver.setConfiguration(config);
        mapDriver.runTest();
    }

    @Test
    public void returnsWordCountWithEscapePattern() throws IOException {
        final Text value = new Text("Hello Hadoop.\n Hadoop");
        final List<Pair<Text, IntWritable>> mapResult = new ArrayList<>();

        final Pair<Text, IntWritable> word1 = new Pair<>(new Text("Hello"), new IntWritable(1));
        final Pair<Text, IntWritable> word2 = new Pair<>(new Text("Hadoop"), new IntWritable(1));

        mapResult.add(word1);
        mapResult.add(word2);
        mapResult.add(word2);

        Configuration config = new Configuration();
        config.set("wordCount.skipPattern.file", "input/wordcount/pattern.txt");

        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), value)
                .withAllOutput(mapResult);

        mapDriver.setConfiguration(config);
        mapDriver.runTest();
    }

    @Test
    public void returnsWordCountWithEscapePatternAndIgnoreCase() throws IOException {
        final Text value = new Text("Hello Hadoop.\n Hadoop");
        final List<Pair<Text, IntWritable>> mapResult = new ArrayList<>();

        final Pair<Text, IntWritable> word1 = new Pair<>(new Text("hello"), new IntWritable(1));
        final Pair<Text, IntWritable> word2 = new Pair<>(new Text("hadoop"), new IntWritable(1));

        mapResult.add(word1);
        mapResult.add(word2);
        mapResult.add(word2);

        Configuration config = new Configuration();
        config.set("wordCount.ignoreCase", "true");
        config.set("wordCount.skipPattern.file", "input/wordcount/pattern.txt");

        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), value)
                .withAllOutput(mapResult);

        mapDriver.setConfiguration(config);
        mapDriver.runTest();
    }

}
