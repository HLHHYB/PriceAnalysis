package org.example.PriceAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * PriceStatsReducer class
 * Reduces the price values to calculate statistics (min, max, avg)
 * for either route-date combinations or airlines
 *
 * Uses Welford's online algorithm for efficient standard deviation calculation
 * without storing all prices in memory
 */
public class PriceStatsReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double minPrice = Double.MAX_VALUE;
        double maxPrice = Double.MIN_VALUE;
        double mean = 0.0;
        double m2 = 0.0;  // Sum of squared differences from the current mean
        int count = 0;

        // Process all price values for this key using Welford's online algorithm
        for (Text value : values) {
            try {
                double price = Double.parseDouble(value.toString());

                // Update min and max
                minPrice = Math.min(minPrice, price);
                maxPrice = Math.max(maxPrice, price);

                // Update count
                count++;

                // Welford's online algorithm for variance calculation
                double delta = price - mean;
                mean += delta / count;
                double delta2 = price - mean;
                m2 += delta * delta2;

            } catch (NumberFormatException e) {
                // Skip invalid price values
                System.err.println("Invalid price value: " + value.toString());
            }
        }

        // Only output results if we have valid price data
        if (count > 0) {
            // Calculate standard deviation
            double variance = (count > 1) ? m2 / count : 0;
            double stdDev = Math.sqrt(variance);

            // Format the output
            String stats = String.format("min=%.2f,max=%.2f,avg=%.2f,count=%d,stdDev=%.2f",
                    minPrice, maxPrice, mean, count, stdDev);
            outputValue.set(stats);

            // Emit the key with aggregated statistics
            context.write(key, outputValue);
        }
    }
}