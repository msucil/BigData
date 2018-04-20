package com.msucil.hadoop.wordcount.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by msucil on 4/20/18.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, ONE);
        }
    }
}
