package com.msucil.hadoop.wordcount.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by msucil on 4/20/18.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final String EMPTY = "";

    private boolean ignoreCase;

    private final Set<String> patternsToSkip = new HashSet<>();

    private Text word = new Text();
    private final IntWritable DEFAULT_COUNT = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        ignoreCase = Boolean.valueOf(configuration.get("wordCount.ignoreCase"));

        String skipPatternFile = configuration.get("wordCount.skipPattern.file");

        if(null != skipPatternFile) {
            setSkipPattern(skipPatternFile);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = (ignoreCase) ? value.toString().toLowerCase() : value.toString();

        for(String pattern : patternsToSkip) {
            line = line.replaceAll(pattern, EMPTY);
        }

        final StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());

            context.write(word, DEFAULT_COUNT);
        }
    }

    private void setSkipPattern(String fileName) throws IOException {

        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String pattern;

            while ( (pattern = reader.readLine()) != null) {
                patternsToSkip.add(pattern);
            }
        }
    }
}
