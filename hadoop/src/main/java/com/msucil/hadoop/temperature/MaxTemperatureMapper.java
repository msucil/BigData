package com.msucil.hadoop.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by msucil on 4/20/18.
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    private static final char POSITIVE_SIGN = '+';
    private static final String QUALITY_AIR = "[01459]";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String year = line.substring(15, 19);

        int airTemperature = (line.charAt(87) == POSITIVE_SIGN)
                ? Integer.parseInt(line.substring(88, 92))
                : Integer.parseInt(line.substring(87, 92));

        String airQuality = line.substring(92, 93);

        if(airTemperature != MISSING && airQuality.matches(QUALITY_AIR)) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
