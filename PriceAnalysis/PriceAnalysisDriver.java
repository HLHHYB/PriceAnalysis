package org.example.PriceAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class for the Flight Price Analysis MapReduce jobs
 * This class sets up and executes three MapReduce jobs:
 * 1. Route-Date based price analysis
 * 2. Airline based price analysis
 * 3. Comprehensive insight analysis (combining results from jobs 1 & 2)
 */
public class PriceAnalysisDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PriceAnalysisDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
//        if (args.length != 4) {
//            System.err.println("Usage: PriceAnalysisDriver <input path> <route output> <airline output> <insight output>");
//            return -1;
//        }

//        String inputPath = args[0];
//        String routeOutputPath = args[1];
//        String airlineOutputPath = args[2];
//        String insightOutputPath = args[3];

        String inputPath = "/Users/hlhhyb/Desktop/itineraries.csv";
        String routeOutputPath = "/Users/hlhhyb/Desktop/routeOutput";
        String airlineOutputPath = "/Users/hlhhyb/Desktop/airlineOutput";
        String insightOutputPath = "/Users/hlhhyb/Desktop/PriceInsightOutput";

        System.out.println("=== Starting Flight Price Analysis Pipeline ===");

        // First job: Route-Date based price analysis
        System.out.println("Running Route Analysis Job...");
        boolean routeJobSuccess = runRouteAnalysisJob(inputPath, routeOutputPath);
        if (!routeJobSuccess) {
            System.err.println("Route Analysis Job failed!");
            return -1;
        }
        System.out.println("Route Analysis Job completed successfully!");

        // Second job: Airline based price analysis
        System.out.println("Running Airline Analysis Job...");
        boolean airlineJobSuccess = runAirlineAnalysisJob(inputPath, airlineOutputPath);
        if (!airlineJobSuccess) {
            System.err.println("Airline Analysis Job failed!");
            return -1;
        }
        System.out.println("Airline Analysis Job completed successfully!");

        // Third job: Comprehensive insight analysis
        System.out.println("Running Comprehensive Insight Analysis Job...");
        boolean insightJobSuccess = runInsightAnalysisJob(routeOutputPath, airlineOutputPath, insightOutputPath);
        if (!insightJobSuccess) {
            System.err.println("Insight Analysis Job failed!");
            return -1;
        }
        System.out.println("Insight Analysis Job completed successfully!");

        System.out.println("=== All Analysis Jobs Completed Successfully! ===");
        System.out.println("Check the following directories for results:");
        System.out.println("- Route Analysis: " + routeOutputPath);
        System.out.println("- Airline Analysis: " + airlineOutputPath);
        System.out.println("- Comprehensive Insights: " + insightOutputPath);

        return 0;
    }

    private boolean runRouteAnalysisJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.java.opts", "-Xmx4g");
        conf.set("mapreduce.reduce.memory.mb", "5120");

        Job job = Job.getInstance(conf, "Route-Date Price Analysis");
        job.setJarByClass(PriceAnalysisDriver.class);

        job.setMapperClass(PriceRouteMapper.class);
        job.setReducerClass(PriceStatsReducer.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }

    private boolean runAirlineAnalysisJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.java.opts", "-Xmx4g");
        conf.set("mapreduce.reduce.memory.mb", "5120");

        Job job = Job.getInstance(conf, "Airline Price Analysis");
        job.setJarByClass(PriceAnalysisDriver.class);

        job.setMapperClass(AirlineMapper.class);
        job.setReducerClass(PriceStatsReducer.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }

    /**
     * 运行综合洞察分析作业
     * 读取前两个作业的输出结果，生成具有实际应用价值的分析报告
     */
    private boolean runInsightAnalysisJob(String routeOutputPath, String airlineOutputPath, String insightOutputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.java.opts", "-Xmx4g");
        conf.set("mapreduce.reduce.memory.mb", "5120");

        Job job = Job.getInstance(conf, "Comprehensive Insight Analysis");
        job.setJarByClass(PriceAnalysisDriver.class);

        job.setMapperClass(PriceInsightAnalysisMapper.class);
        job.setReducerClass(PriceInsightAnalysisReducer.class);
        job.setNumReduceTasks(1); // 使用单个reducer确保所有数据集中处理

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 添加航线分析和航空公司分析的输出路径作为输入
        FileInputFormat.addInputPath(job, new Path(routeOutputPath));
        FileInputFormat.addInputPath(job, new Path(airlineOutputPath));
        FileOutputFormat.setOutputPath(job, new Path(insightOutputPath));

        return job.waitForCompletion(true);
    }

}
