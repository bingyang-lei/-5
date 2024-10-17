package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Comparator;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        /**
         * setup 方法在 Map 任务开始前运行，用于加载停词列表。
         *
         * @param context Mapper 上下文
         * @throws IOException
         */
        @Override
        protected void setup(Context context) throws IOException {
            // 从 HDFS 中加载停词列表
            Path stopWordsPath = new Path("hdfs:/lhd/input/stop-word-list.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        }

        /**
         * map 方法读取每一行输入数据，提取单词并输出键值对 (单词, 1)。
         *
         * @param key     输入键
         * @param value   输入值
         * @param context Mapper 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将输入行按逗号分割成字段数组
            String[] fields = value.toString().split(",");
            // 检查字段数组长度是否大于1，以确保包含标题
            if (fields.length > 1) {
                // 将标题转换为小写
                String headline = fields[1].toLowerCase();
                // 按非单词字符分割标题，提取单词
                String[] tokens = headline.split("\\W+");

                // 遍历所有单词
                for (String token : tokens) {
                    // 检查单词是否不在停词列表中且不为空
                    if (!stopWords.contains(token) && !token.isEmpty()) {
                        // 设置单词
                        word.set(token);
                        // 输出键值对 (单词, 1)
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<String, Integer> wordCountMap = new HashMap<>();

        /**
         * reduce 方法汇总每个单词的出现次数，并存储在 Map 中。
         *
         * @param key     输入键（单词）
         * @param values  输入值的迭代器（每个值都是 1）
         * @param context Reducer 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // 遍历所有值，计算总和
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 将结果存储在 Map 中
            wordCountMap.put(key.toString(), sum);
        }

        /**
         * cleanup 方法在所有 reduce 任务完成后运行，用于输出前100个高频单词。
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 使用优先队列对结果进行排序
            PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(
                    new Comparator<Map.Entry<String, Integer>>() {
                        @Override
                        public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
                            return b.getValue() - a.getValue();
                        }
                    });
            pq.addAll(wordCountMap.entrySet());

            int count = 0;
            // 输出前100个高频单词
            while (!pq.isEmpty() && count < 100) {
                Map.Entry<String, Integer> entry = pq.poll();
                context.write(new Text((count + 1) + " : " + entry.getKey()), new IntWritable(entry.getValue()));
                count++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}