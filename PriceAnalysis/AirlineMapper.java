package org.example.PriceAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * AirlineMapper class
 * Maps flight records to airline and price pairs
 * Key: airlineCode (segmentsAirlineCode)
 * Value: totalFare
 */
public class AirlineMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header row
        if (key.get() == 0 && value.toString().contains("legId")) {
            return;
        }

        try {
            String[] fields = value.toString().split(",");

            // Check if we have enough fields
            if (fields.length < 23) {
                return; // Skip malformed records
            }

            String airlineCode = getFieldSafely(fields, 22);
            String totalFare = getFieldSafely(fields, 12);

            // Skip if any required fields are missing
            if (airlineCode.isEmpty() || totalFare.isEmpty()) {
                return;
            }

            // Try to parse the totalFare to ensure it's a valid number
            try {
                Double.parseDouble(totalFare);
            } catch (NumberFormatException e) {
                return; // Skip if totalFare is not a valid number
            }

            // Set the airline code as the key
            outputKey.set(airlineCode);

            // The value is the price
            outputValue.set(totalFare);

            // Emit the key-value pair
            context.write(outputKey, outputValue);

        } catch (Exception e) {
            // Log the error and continue with the next record
            System.err.println("Error processing record: " + e.getMessage());
        }
    }

    /**
     * Helper method to safely get a field from the array, handling index out of bounds
     */
    private String getFieldSafely(String[] fields, int index) {
        if (index >= 0 && index < fields.length) {
            return fields[index].trim();
        }
        return "";
    }
}