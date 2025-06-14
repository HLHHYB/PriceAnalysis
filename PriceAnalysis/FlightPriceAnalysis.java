package org.example.PriceAnalysis;//package org.example.PriceAnalysis;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//
///**
// * Example usage class with main method for running the flight price analysis
// */
//public class FlightPriceAnalysis {
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//
//        if (otherArgs.length != 3) {
//            System.err.println("Usage: FlightPriceAnalysis <input path> <output path for route analysis> <output path for airline analysis>");
//            System.exit(2);
//        }
//
//        // Run the driver
//        int exitCode = PriceAnalysisDriver.main(otherArgs);
//        System.exit(exitCode);
//    }
//}
