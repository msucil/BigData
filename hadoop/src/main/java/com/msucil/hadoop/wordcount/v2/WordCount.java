package com.msucil.hadoop.wordcount.v2;

import com.msucil.hadoop.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by msucil on 4/20/18.
 *
 * Usage: hadoop jar hadoop-examples.jar com.msucil.hadoop.wordcount.v2.WordCount [-DwordCount.ignoreCase=true] [-DwordCount.skipPattern.file=<file path>] <input path> <output path>

 */
public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        final Configuration config = getConf();
        final GenericOptionsParser optionsParser = new GenericOptionsParser(config, args);

        final String[] actualArguments = optionsParser.getRemainingArgs();

        if(actualArguments.length != 2) {
            System.err.println("Usage: hadoop jar hadoop-examples.jar com.msucil.hadoop.wordcount.v2.WordCount [-DwordCount.ignoreCase=true] [-DwordCount.skipPattern.file=<file path>] <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(config);
        job.setJarByClass(WordCount.class);
        job.setJobName("Word Count V2");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(actualArguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(actualArguments[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
