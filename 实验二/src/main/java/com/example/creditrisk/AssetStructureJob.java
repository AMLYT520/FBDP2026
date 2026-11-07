package com.example.creditrisk;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task 4: quartile-based groups and their 7-category asset distribution.
 * Group order is set by IntWritable (0..3); per-group categories by descending ratio.
 */
public class AssetStructureJob {
    static final String[] LEVELS = {"低资产", "中等资产", "较高资产", "高资产"};

    public static class StructureMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private BigDecimal q25, q50, q75;

        @Override
        protected void setup(Context ctx) throws IOException {
            try (BufferedReader reader = JobUtils.readCache("quartiles-cache.tsv")) {
                String line = reader.readLine();
                if (line == null) throw new IOException("Empty task3 quartiles result");
                String[] items = line.split("\t", -1);
                if (items.length != 3 || !items[0].startsWith("Q25:")
                        || !items[1].startsWith("Q50:")
                        || !items[2].startsWith("Q75:")) {
                    throw new IOException("Unexpected task3 output: " + line);
                }
                q25 = new BigDecimal(items[0].substring(4));
                q50 = new BigDecimal(items[1].substring(4));
                q75 = new BigDecimal(items[2].substring(4));
                if (q25.compareTo(q50) > 0 || q50.compareTo(q75) > 0) {
                    throw new IOException("Quartiles are not monotonic");
                }
            }
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

            int level = row.asset.compareTo(q25) < 0 ? 0
                    : row.asset.compareTo(q50) < 0 ? 1
                    : row.asset.compareTo(q75) < 0 ? 2 : 3;

            StringBuilder seven = new StringBuilder();
            for (int i = 0; i < 7; ++i) {
                if (i > 0) seven.append(',');
                seven.append(TrainRow.money(row.categories[i]));
            }
            ctx.write(new IntWritable(level), new Text(seven.toString()));
        }
    }

    static final class Ratio {
        final int originalIndex;
        final BigDecimal value;
        Ratio(int originalIndex, BigDecimal value) {
            this.originalIndex = originalIndex;
            this.value = value;
        }
    }

    public static class StructureReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            int level = key.get();
            if (level < 0 || level >= LEVELS.length)
                throw new IOException("Unknown level " + level);
            BigDecimal[] sums = new BigDecimal[7];
            for (int i = 0; i < 7; ++i) sums[i] = BigDecimal.ZERO;

            for (Text item : values) {
                String[] cells = item.toString().split(",", -1);
                if (cells.length != 7)
                    throw new IOException("Malformed structure record: " + item);
                for (int i = 0; i < 7; ++i) {
                    sums[i] = sums[i].add(new BigDecimal(cells[i]));
                }
            }
            BigDecimal denominator = BigDecimal.ZERO;
            for (BigDecimal x : sums) denominator = denominator.add(x);

            List<Ratio> ratios = new ArrayList<>();
            for (int i = 0; i < 7; ++i) {
                BigDecimal share = denominator.signum() == 0
                        ? BigDecimal.ZERO
                        : sums[i].divide(denominator, 12, RoundingMode.HALF_UP);
                ratios.add(new Ratio(i, share));
            }
            ratios.sort(Comparator.comparing((Ratio x) -> x.value).reversed()
                    .thenComparingInt(x -> x.originalIndex));

            for (Ratio r : ratios) {
                String category = TrainRow.CATEGORY_NAMES[r.originalIndex];
                String share = r.value.setScale(4, RoundingMode.HALF_UP)
                        .toPlainString();
                ctx.write(new Text(LEVELS[level] + "\t" + category),
                        new Text(share));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobUtils.checkArgs(args, 3,
                "AssetStructureJob <train.csv> <output/task3> <output/task4>");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task4-credit-asset-structure");
        job.setJarByClass(AssetStructureJob.class);
        job.setMapperClass(StructureMapper.class);
        job.setReducerClass(StructureReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        JobUtils.cache(job, new Path(args[1], "part-r-00000"), "quartiles-cache.tsv");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
