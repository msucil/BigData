package com.msucil.hadoop.wordcount.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by msucil on 4/22/18.
 */
public class WordCountTest {

    @Test
    public void processOnLocalDataWithDefaultInputs() throws Exception {
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

    @Test
    public void processOnLocalDataWithIgnoreCase() throws Exception {
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

        int exitCode = wordCount.run(new String[]{"-DwordCount.ignoreCase=true", input.toString(), outout.toString()});

        assertEquals(exitCode, 0);
    }

    @Test
    public void processOnLocalDataWithEscapePattern() throws Exception {
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

        int exitCode = wordCount.run(new String[]{"-DwordCount.skipPattern.file=input/wordcount/pattern.txt", input.toString(), outout.toString()});

        assertEquals(exitCode, 0);
    }

    @Test
    public void processOnLocalDataWithAllPossibleArguments() throws Exception {
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

        int exitCode = wordCount.run(new String[]{"-DwordCount.ignoreCase=true" ,"-DwordCount.skipPattern.file=input/wordcount/pattern.txt", input.toString(), outout.toString()});

        assertEquals(exitCode, 0);
    }
}
