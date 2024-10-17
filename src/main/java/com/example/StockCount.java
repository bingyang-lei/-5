package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * StockCount 是一个 MapReduce 程序，用于统计股票代码的出现次数，并按出现次数从大到小输出。
 */
public class StockCount {

    /**
     * StockMapper 是 Mapper 类，用于读取输入数据并提取股票代码。
     */
    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text stockCode = new Text();
        private final static IntWritable one = new IntWritable(1);
        private int lineCount = 0; // 用于跟踪已处理的行数
    
        /**
         * map 方法读取每一行输入数据，提取股票代码并输出键值对 (股票代码, 1)。
         *
         * @param key 输入键
         * @param value 输入值
         * @param context Mapper 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 抛弃输入的第一行
            if (value.toString().contains("index")) {
                return;
            }
    
            // // 只处理前10行
            // if (lineCount >= 10) {
            //     return;
            // }
    
            // 将输入行按逗号分割成字段数组
            String[] fields = value.toString().split(",");
            // 检查字段数组长度是否大于3，以确保包含股票代码
            // 设置股票代码,为字段数组的最后一个元素
            stockCode.set(fields[fields.length - 1].trim());
            // 输出键值对 (股票代码, 1)
            context.write(stockCode, one);
            
    
            // // 增加已处理的行数
            // lineCount++;
        }
    }

    /**
     * StockReducer 是 Reducer 类，用于汇总每个股票代码的出现次数。
     */
    public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> stockCountMap = new HashMap<>();

        /**
         * reduce 方法汇总每个股票代码的出现次数，并存储在 Map 中。
         *
         * @param key 输入键（股票代码）
         * @param values 输入值的迭代器（每个值都是 1）
         * @param context Reducer 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // 遍历所有值，计算总和
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 将结果存储在 Map 中
            stockCountMap.put(key.toString(), sum);
        }

        /**
         * cleanup 方法在所有 reduce 任务完成后运行，用于对结果进行排序并输出。
         *
         * @param context Reducer 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 使用优先队列对结果进行排序
            PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>((a, b) -> b.getValue() - a.getValue());
            pq.addAll(stockCountMap.entrySet());

            int rank = 1;
            // 输出排序后的结果
            while (!pq.isEmpty()) {
                Map.Entry<String, Integer> entry = pq.poll();
                context.write(new Text(rank + ":" + entry.getKey()), new IntWritable(entry.getValue()));
                rank++;
            }
        }
    }

    /**
     * main 方法设置并运行 MapReduce 作业。
     *
     * @param args 输入参数，第一个参数是输入路径，第二个参数是输出路径
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 创建 Hadoop 配置对象
        Configuration conf = new Configuration();
        // 创建一个新的 Job
        Job job = Job.getInstance(conf, "Stock Count");
        // 设置 Job 的主类
        job.setJarByClass(StockCount.class);
        // 设置 Mapper 类
        job.setMapperClass(StockMapper.class);
        // 设置 Reducer 类
        job.setReducerClass(StockReducer.class);
        // 设置输出键的类型
        job.setOutputKeyClass(Text.class);
        // 设置输出值的类型
        job.setOutputValueClass(IntWritable.class);
        // 设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交 Job 并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}