package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesByYearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private String targetYear;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // Get the target year from the job configuration
        targetYear = context.getConfiguration().get("targetYear");
    }
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // Split the input line
        String[] fields = value.toString().split("\\s+");

        String date = fields[0];     // Date in the format YYYY-MM-DD
        String city = fields[1];     // City name
        String price = fields[3];    // Sales price

        // Extract year from date
        String year = date.split("-")[0];

        // Only process records matching the target year
        if (year.equals(targetYear)) {
            // Emit city as the key and price as the value
            context.write(new Text(city), new DoubleWritable(Integer.parseInt(price)));
        }
    }
}
