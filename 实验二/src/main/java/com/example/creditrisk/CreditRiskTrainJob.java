package com.example.creditrisk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task5 training: categorical Naive Bayes sufficient statistics.
 * Model output: <key>TAB<count>
 *   C|0 -> count of label 0
 *   C|1 -> count of label 1
 *   F|0|featureIndex|value -> class-conditional frequency
 */
public class CreditRiskTrainJob {

    public static class TrainMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {
        private CreditRiskFeatures.Quartiles quartiles;
        private static final LongWritable ONE = new LongWritable(1L);

        @Override
        protected void setup(Context context) throws IOException {
            quartiles = CreditRiskFeatures.Quartiles.load("credit-quartiles.tsv");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            final CreditRiskFeatures.Row row;
            try {
                row = CreditRiskFeatures.parse(value.toString(), true, quartiles);
            } catch (IllegalArgumentException ex) {
                context.getCounter("TASK5", "INVALID_TRAIN_ROWS").increment(1);
                return;
            }
            if (row == null) return;
            String label = Integer.toString(row.label);
            context.write(new Text("C|" + label), ONE);
            for (int i = 0; i < row.features.length; ++i) {
                context.write(
                        new Text("F|" + label + "|" + i + "|" + row.features[i]), ONE);
            }
            context.getCounter("TASK5", "VALID_TRAIN_ROWS").increment(1);
        }
    }

    public static class SumReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable x : values) {
                try {
                    sum = Math.addExact(sum, x.get());
                } catch (ArithmeticException ex) {
                    throw new IOException("Count overflow for " + key, ex);
                }
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CreditRiskTrainJob "
                    + "<hdfs_train_csv> <hdfs_task3_output_dir> <hdfs_model_output_dir>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "task5-credit-nb-training");
        job.setJarByClass(CreditRiskTrainJob.class);
        job.setMapperClass(TrainMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1);

        CreditRiskFeatures.addCacheFile(job,
                new Path(args[1], "part-r-00000"), "credit-quartiles.tsv");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
