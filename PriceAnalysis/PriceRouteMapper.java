package org.example.PriceAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * RouteMapper class
 * Maps flight records to route-date and price pairs
 * Key: startingAirport-destinationAirport_flightDate
 * Value: totalFare
 */
public class PriceRouteMapper extends Mapper<LongWritable, Text, Text, Text> {

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

            // Check if we have enough fields and required fields are not empty
            if (fields.length < 15) {
                return; // Skip malformed records
            }

            String startingAirport = getFieldSafely(fields, 3);
            String destinationAirport = getFieldSafely(fields, 4);
            String flightDate = getFieldSafely(fields, 2);
            String totalFare = getFieldSafely(fields, 12);

            // Skip if any required fields are missing
            if (startingAirport.isEmpty() || destinationAirport.isEmpty() ||
                    flightDate.isEmpty() || totalFare.isEmpty()) {
                return;
            }

            // Try to parse the totalFare to ensure it's a valid number
            try {
                Double.parseDouble(totalFare);
            } catch (NumberFormatException e) {
                return; // Skip if totalFare is not a valid number
            }

            // Create the composite key: route_date
            String routeDateKey = startingAirport + "-" + destinationAirport + "_" + flightDate;
            outputKey.set(routeDateKey);

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
