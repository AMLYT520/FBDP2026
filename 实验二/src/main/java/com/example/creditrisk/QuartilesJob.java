package com.example.creditrisk;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task 3: key=DoubleWritable(asset), one reducer, so Shuffle does global sorting.
 * Uses valid record count from task1 to locate percentiles by (N-1)*p,
 * with linear interpolation between adjacent order statistics.
 */
public class QuartilesJob {
    private static final String COUNT_KEY = "credit.valid.customer.count";

    public static class AssetMapper
            extends Mapper<LongWritable, Text, DoubleWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {
            try {
                TrainRow row = TrainRow.parse(value.toString());
                if (row == null) return;
                double asset = row.asset.doubleValue();
                if (!Double.isFinite(asset)) {
                    throw new IllegalArgumentException("Asset outside double range");
                }
                ctx.write(new DoubleWritable(asset), NullWritable.get());
                ctx.getCounter("CSV", "VALID_ROWS").increment(1);
            } catch (IllegalArgumentException ex) {
                ctx.getCounter("CSV", "INVALID_ROWS").increment(1);
            }
        }
    }

    public static class QuartileReducer
            extends Reducer<DoubleWritable, NullWritable, Text, NullWritable> {
        private final double[] p = {0.25d, 0.50d, 0.75d};
        private final long[] lowRank = new long[3];
        private final long[] highRank = new long[3];
        private final BigDecimal[] lowValue = new BigDecimal[3];
        private final BigDecimal[] highValue = new BigDecimal[3];
        private final BigDecimal[] frac = new BigDecimal[3];
        private long expectedN;
        private long offset = 0;

        @Override
        protected void setup(Context ctx) throws IOException {
            expectedN = ctx.getConfiguration().getLong(COUNT_KEY, -1);
            if (expectedN <= 0) throw new IOException("Missing positive customer count");
            for (int i = 0; i < 3; ++i) {
                // Multiply after converting to BigDecimal to keep positions exact.
                BigDecimal rank = BigDecimal.valueOf(expectedN - 1)
                        .multiply(BigDecimal.valueOf(p[i]));
                lowRank[i] = rank.toBigInteger().longValueExact();
                highRank[i] = rank.signum() >= 0 && rank.remainder(BigDecimal.ONE).signum() != 0
                        ? lowRank[i] + 1 : lowRank[i];
                frac[i] = rank.subtract(BigDecimal.valueOf(lowRank[i]));
            }
        }

        @Override
        protected void reduce(DoubleWritable key, Iterable<NullWritable> values,
                              Context ctx) throws IOException {
            long frequency = 0;
            for (NullWritable ignored : values) frequency++;
            long end = offset + frequency - 1;
            BigDecimal number = BigDecimal.valueOf(key.get());
            for (int i = 0; i < 3; ++i) {
                if (offset <= lowRank[i] && lowRank[i] <= end) lowValue[i] = number;
                if (offset <= highRank[i] && highRank[i] <= end) highValue[i] = number;
            }
            offset += frequency;
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            if (offset != expectedN) {
                throw new IOException("Valid record count differs from task1: expected "
                        + expectedN + ", got " + offset);
            }
            BigDecimal[] quantiles = new BigDecimal[3];
            for (int i = 0; i < 3; ++i) {
                if (lowValue[i] == null || highValue[i] == null)
                    throw new IOException("Missing sorted order statistic " + i);
                quantiles[i] = lowValue[i].add(
                        highValue[i].subtract(lowValue[i]).multiply(frac[i]));
            }
            String line = "Q25:" + TrainRow.money(quantiles[0])
                    + "\tQ50:" + TrainRow.money(quantiles[1])
                    + "\tQ75:" + TrainRow.money(quantiles[2]);
            ctx.write(new Text(line), NullWritable.get());
        }
    }

    static long countCustomers(Configuration conf, Path task1File) throws IOException {
        long count = 0;
        try (BufferedReader reader = JobUtils.readHdfs(conf, task1File)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] kv = line.split("\t", -1);
                if (kv.length != 2) throw new IOException("Bad task1 line: " + line);
                String[] stats = kv[1].split(",", -1);
                if (stats.length != 4) throw new IOException("Bad task1 stats: " + line);
                count = Math.addExact(count, Long.parseLong(stats[0]));
            }
        }
        if (count <= 0) throw new IOException("Task1 count is zero");
        return count;
    }

    public static void main(String[] args) throws Exception {
        JobUtils.checkArgs(args, 3,
                "QuartilesJob <train.csv> <output/task1> <output/task3>");
        Configuration conf = new Configuration();
        long n = countCustomers(conf, new Path(args[1], "part-r-00000"));
        conf.setLong(COUNT_KEY, n);

        Job job = Job.getInstance(conf, "task3-credit-asset-quartiles");
        job.setJarByClass(QuartilesJob.class);
        job.setMapperClass(AssetMapper.class);
        job.setReducerClass(QuartileReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.out.println("Valid customer count (from task1): " + n);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
