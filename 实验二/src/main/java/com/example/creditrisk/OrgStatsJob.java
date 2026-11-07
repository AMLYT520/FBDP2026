package com.example.creditrisk;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Task 1: organization count, total assets, total debt, and bad customer count. */
public class OrgStatsJob {
    public static class StatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {
            try {
                TrainRow r = TrainRow.parse(value.toString());
                if (r == null) return;
                String record = "1," + TrainRow.money(r.asset) + ","
                        + TrainRow.money(r.debt) + "," + (r.label == 1 ? "1" : "0");
                ctx.write(new Text(r.org), new Text(record));
                ctx.getCounter("CSV", "VALID_ROWS").increment(1);
            } catch (IllegalArgumentException ex) {
                ctx.getCounter("CSV", "INVALID_ROWS").increment(1);
            }
        }
    }

    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text org, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            long customers = 0L, bad = 0L;
            BigDecimal asset = BigDecimal.ZERO;
            BigDecimal debt = BigDecimal.ZERO;

            for (Text item : values) {
                String[] pieces = item.toString().split(",", -1);
                customers += Long.parseLong(pieces[0]);
                asset = asset.add(new BigDecimal(pieces[1]));
                debt = debt.add(new BigDecimal(pieces[2]));
                bad += Long.parseLong(pieces[3]);
            }
            ctx.write(org, new Text(customers + "," + TrainRow.money(asset)
                    + "," + TrainRow.money(debt) + "," + bad));
        }
    }

    public static void main(String[] args) throws Exception {
        JobUtils.checkArgs(args, 2, "OrgStatsJob <train.csv> <output/task1>");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task1-credit-org-statistics");
        job.setJarByClass(OrgStatsJob.class);
        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
