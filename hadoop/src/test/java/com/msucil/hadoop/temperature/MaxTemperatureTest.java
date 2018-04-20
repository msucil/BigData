package com.msucil.hadoop.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import java.io.IOException;

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
}
