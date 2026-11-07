package com.example.creditrisk;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

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
 * Task 2: uses task1 output (distributed cache) for each organization's mean.
 * Exact comparison: customerAsset * customerCount >= totalOrgAsset.
 */
public class OrgAverageJob {
    private static class Totals {
        final long count;
        final BigDecimal asset;

        Totals(long count, BigDecimal asset) {
            this.count = count;
            this.asset = asset;
        }
    }

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, Totals> stats = new HashMap<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            try (BufferedReader reader = JobUtils.readCache("org-stats-cache.tsv")) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) continue;
                    String[] kv = line.split("\t", -1);
                    if (kv.length != 2) {
                        throw new IOException("Bad task1 output: " + line);
                    }
                    String[] v = kv[1].split(",", -1);
                    if (v.length != 4) {
                        throw new IOException("Bad task1 value: " + line);
                    }
                    long count = Long.parseLong(v[0]);
                    if (count <= 0) throw new IOException("Non-positive count for " + kv[0]);
                    stats.put(kv[0], new Totals(count, new BigDecimal(v[1])));
                }
            }
            if (stats.isEmpty()) throw new IOException("Task1 result contains no organizations");
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {
            final TrainRow row;
            try {
                row = TrainRow.parse(value.toString());
            } catch (IllegalArgumentException ex) {
                ctx.getCounter("CSV", "INVALID_ROWS").increment(1);
                return;
            }
            if (row == null) return;

            Totals t = stats.get(row.org);
            if (t == null) {
                throw new IOException("No organization mean in task1 output for " + row.org);
            }
            // Avoid division/rounding; equality belongs to the "at or above" group.
            boolean atOrAbove = row.asset.multiply(BigDecimal.valueOf(t.count))
                    .compareTo(t.asset) >= 0;
            ctx.write(new Text(row.org), new Text(atOrAbove ? "1,0" : "0,1"));
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            long atOrAbove = 0, below = 0;
            for (Text value : values) {
                String[] v = value.toString().split(",", -1);
                atOrAbove += Long.parseLong(v[0]);
                below += Long.parseLong(v[1]);
            }
            ctx.write(key, new Text(atOrAbove + "," + below));
        }
    }

    public static void main(String[] args) throws Exception {
        JobUtils.checkArgs(args, 3,
                "OrgAverageJob <train.csv> <output/task1> <output/task2>");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task2-credit-org-mean-comparison");
        job.setJarByClass(OrgAverageJob.class);
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        JobUtils.cache(job, new Path(args[1], "part-r-00000"), "org-stats-cache.tsv");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
