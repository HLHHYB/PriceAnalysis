package org.example.PriceAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * InsightMapper class
 * 读取航线分析和航空公司分析的结果，为综合洞察分析做准备
 *
 * 输入来源标识：
 * - route: 来自航线分析结果
 * - airline: 来自航空公司分析结果
 *
 * Key: 分析类型 (route/airline)
 * Value: 原始数据行
 */
public class PriceInsightAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 跳过空行
        if (value.toString().trim().isEmpty()) {
            return;
        }

        try {
            // 获取输入文件名来确定数据来源
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            String dataSource;
            if (fileName.contains("route") || fileSplit.getPath().toString().contains("routeOutput")) {
                dataSource = "route";
            } else if (fileName.contains("airline") || fileSplit.getPath().toString().contains("airlineOutput")) {
                dataSource = "airline";
            } else {
                dataSource = "unknown";
            }

            // 解析数据行
            String line = value.toString().trim();
            String[] parts = line.split("\t");

            if (parts.length >= 2) {
                String identifier = parts[0]; // 航线信息或航空公司代码
                String stats = parts[1];      // 统计信息

                // 解析统计信息
                if (stats.contains("min=") && stats.contains("max=") && stats.contains("avg=")) {
                    // 构造输出键：数据源类型
                    outputKey.set(dataSource);

                    // 构造输出值：标识符|统计信息
                    outputValue.set(identifier + "|" + stats);

                    context.write(outputKey, outputValue);
                }
            }

        } catch (Exception e) {
            System.err.println("Error processing insight record: " + e.getMessage());
        }
    }
}