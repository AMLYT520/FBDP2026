package com.example.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Two consecutive MapReduce jobs for the stock-set assignment. */
public final class StockSetJobs {
    private StockSetJobs() {}

    public abstract static class SourceMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected abstract String sourceName();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            try {
                StockLogic.Record r = StockLogic.parseInput(line.toString());
                if (r == null) return;
                // Composite sort key: index + TAB + stock ticker.
                ctx.write(new Text(r.index + "\t" + r.ticker), new Text(sourceName()));
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid " + sourceName() + " record at byte "
                        + offset.get() + ": " + line, e);
            }
        }
    }

    public static final class AMapper extends SourceMapper {
        @Override protected String sourceName() { return "A"; }
    }

    public static final class BMapper extends SourceMapper {
        @Override protected String sourceName() { return "B"; }
    }

    public static final class RelationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text stockKey, Iterable<Text> sources, Context ctx)
                throws IOException, InterruptedException {
            boolean inA = false;
            boolean inB = false;
            for (Text source : sources) {
                if (source.toString().equals("A")) inA = true;
                else if (source.toString().equals("B")) inB = true;
                else throw new IOException("Unexpected source: " + source);
            }
            // Duplicates in the same input file do not affect these booleans.
            ctx.write(stockKey, new Text(StockLogic.relation(inA, inB)));
        }
    }

    public static final class StatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            try {
                StockLogic.TaggedRecord r = StockLogic.parseTagged(line.toString());
                if (r == null) return;
                ctx.write(new Text(r.index), new Text(r.tag));
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid task 1 output at byte " + offset.get()
                        + ": " + line, e);
            }
        }
    }

    public static final class StatsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text index, Iterable<Text> tags, Context ctx)
                throws IOException, InterruptedException {
            StockLogic.Stats stats = new StockLogic.Stats();
            for (Text tag : tags) {
                stats.add(tag.toString());
            }
            ctx.write(index, new Text(stats.asOutput()));
        }
    }

    public static int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: hadoop jar <jar> <A.txt> <B.txt> "
                    + "<output_task1> <output_task2>");
            return 2;
        }
        Configuration conf = new Configuration();

        Job task1 = Job.getInstance(conf, "task1-stock-set-relations");
        task1.setJarByClass(StockSetJobs.class);
        MultipleInputs.addInputPath(task1, new Path(args[0]),
                TextInputFormat.class, AMapper.class);
        MultipleInputs.addInputPath(task1, new Path(args[1]),
                TextInputFormat.class, BMapper.class);
        task1.setReducerClass(RelationReducer.class);
        task1.setMapOutputKeyClass(Text.class);
        task1.setMapOutputValueClass(Text.class);
        task1.setOutputKeyClass(Text.class);
        task1.setOutputValueClass(Text.class);
        task1.setNumReduceTasks(1); // one global lexicographically sorted part file
        FileOutputFormat.setOutputPath(task1, new Path(args[2]));

        if (!task1.waitForCompletion(true)) return 1;

        Job task2 = Job.getInstance(conf, "task2-stock-set-statistics");
        task2.setJarByClass(StockSetJobs.class);
        task2.setMapperClass(StatsMapper.class);
        task2.setReducerClass(StatsReducer.class);
        task2.setMapOutputKeyClass(Text.class);
        task2.setMapOutputValueClass(Text.class);
        task2.setOutputKeyClass(Text.class);
        task2.setOutputValueClass(Text.class);
        task2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(task2, new Path(args[2]));
        FileOutputFormat.setOutputPath(task2, new Path(args[3]));

        return task2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }
}
