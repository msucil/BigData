package com.msucil.hadoop.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by msucil on 4/20/18.
 */
public class MaxTemperatureTest {
    private final MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
            MapReduceDriver.newMapReduceDriver(new MaxTemperatureMapper(), new MaxTemperatureReducer());

    @Test
    public void processValidRecordReturnsMaxTemperatureForYear() throws IOException {
        final Text value = new Text("0043011990999991950051518004+68750+023550FM-12" +
                "+038299999V0203201N00261220001CN9999999N9-00111+99999999999");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void ignoreInvalidRecords() throws IOException {
        final Text value =  new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .runTest();
    }

    @Test
    public void processLocalData() throws Exception {
        Configuration config = new Configuration();

        config.set("fs.defaultFS", "local");
        config.set("mapreduce.framework.name", "local");
        config.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("input/ncdc-weather-data.txt");
        Path outout = new Path("output");

        FileSystem fs = FileSystem.getLocal(config);
        fs.delete(outout, true);

        MaxTemperature maxTemperature = new MaxTemperature();
        maxTemperature.setConf(config);

        int exitCode = maxTemperature.run(new String[]{input.toString(), outout.toString()});

        assertEquals(exitCode, 0);
    }
}
