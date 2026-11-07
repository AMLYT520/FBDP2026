package com.example.creditrisk;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * Task5 prediction: load the model through the Hadoop distributed cache and
 * emit CUST_ID<TAB>predicted_bad_good for test.csv (without a label column).
 */
public class CreditRiskPredictJob {
    static final class Model {
        final long[] classCount = new long[2];
        final List<Map<String, long[]>> counts = new ArrayList<>();
        final int[] vocabSize = new int[CreditRiskFeatures.FEATURES];

        Model() {
            for (int i = 0; i < CreditRiskFeatures.FEATURES; ++i) {
                counts.add(new HashMap<>());
            }
        }

        static Model load(String localAlias) throws IOException {
            Model model = new Model();
            try (BufferedReader reader = CreditRiskFeatures.localReader(localAlias)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) continue;
                    String[] columns = line.split("\\t", -1);
                    if (columns.length != 2) {
                        throw new IOException("Bad model line (expected 2 columns): " + line);
                    }
                    long count;
                    try {
                        count = Long.parseLong(columns[1]);
                    } catch (NumberFormatException ex) {
                        throw new IOException("Bad count in model: " + line, ex);
                    }
                    if (count < 0) throw new IOException("Negative model count: " + line);

                    String[] key = columns[0].split("\\|", -1);
                    if (key.length == 2 && key[0].equals("C")) {
                        int clazz = parseClazz(key[1]);
                        model.classCount[clazz] = Math.addExact(model.classCount[clazz], count);
                    } else if (key.length == 4 && key[0].equals("F")) {
                        int clazz = parseClazz(key[1]);
                        int idx;
                        try {
                            idx = Integer.parseInt(key[2]);
                        } catch (NumberFormatException ex) {
                            throw new IOException("Bad feature index: " + line, ex);
                        }
                        if (idx < 0 || idx >= CreditRiskFeatures.FEATURES) {
                            throw new IOException("Unexpected feature index: " + idx);
                        }
                        long[] byClass = model.counts.get(idx)
                                .computeIfAbsent(key[3], ignored -> new long[2]);
                        byClass[clazz] = Math.addExact(byClass[clazz], count);
                    } else {
                        throw new IOException("Invalid model key: " + line);
                    }
                }
            }

            if (model.classCount[0] <= 0 || model.classCount[1] <= 0) {
                throw new IOException("Training data must contain both labels 0 and 1. "
                        + "Class counts: " + model.classCount[0] + ", " + model.classCount[1]);
            }

            for (int i = 0; i < CreditRiskFeatures.FEATURES; ++i) {
                // +1 reserves a category for unseen test values.
                model.vocabSize[i] = model.counts.get(i).size() + 1;
                if (model.vocabSize[i] <= 1) {
                    throw new IOException("Empty feature statistics for feature " + i);
                }
                long[] featureTotals = new long[2];
                for (long[] n : model.counts.get(i).values()) {
                    featureTotals[0] += n[0];
                    featureTotals[1] += n[1];
                }
                if (featureTotals[0] != model.classCount[0]
                        || featureTotals[1] != model.classCount[1]) {
                    throw new IOException("Incomplete model for feature " + i);
                }
            }
            return model;
        }

        private static int parseClazz(String raw) throws IOException {
            if (!raw.equals("0") && !raw.equals("1")) {
                throw new IOException("Unknown class: " + raw);
            }
            return Integer.parseInt(raw);
        }

        int predict(String[] features) {
            long n = classCount[0] + classCount[1];
            double[] logProb = new double[2];
            for (int clazz = 0; clazz < 2; ++clazz) {
                logProb[clazz] = Math.log((classCount[clazz] + 1.0) / (n + 2.0));
                for (int i = 0; i < features.length; ++i) {
                    long[] obs = counts.get(i).get(features[i]);
                    long freq = obs == null ? 0L : obs[clazz];
                    // Laplace (+1) smoothing for categorical likelihood.
                    logProb[clazz] += Math.log((freq + 1.0)
                            / (classCount[clazz] + (double) vocabSize[i]));
                }
            }
            return logProb[1] > logProb[0] ? 1 : 0;
        }
    }

    public static class PredictMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private CreditRiskFeatures.Quartiles quartiles;
        private Model model;

        @Override
        protected void setup(Context context) throws IOException {
            quartiles = CreditRiskFeatures.Quartiles.load("credit-quartiles.tsv");
            model = Model.load("credit-risk-model.tsv");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            final CreditRiskFeatures.Row row;
            try {
                row = CreditRiskFeatures.parse(value.toString(), false, quartiles);
            } catch (IllegalArgumentException ex) {
                context.getCounter("TASK5", "INVALID_TEST_ROWS").increment(1);
                return;
            }
            if (row == null) return;
            int prediction = model.predict(row.features);
            context.write(new Text(row.customerId), new Text(Integer.toString(prediction)));
            context.getCounter("TASK5", prediction == 1 ? "PREDICTED_1" : "PREDICTED_0")
                    .increment(1);
        }
    }

    public static class PredictionReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text id, Iterable<Text> predictions, Context context)
                throws IOException, InterruptedException {
            long encountered = 0;
            for (Text label : predictions) {
                context.write(id, label);
                ++encountered;
            }
            if (encountered > 1) {
                context.getCounter("TASK5", "DUPLICATE_TEST_IDS").increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: CreditRiskPredictJob "
                    + "<hdfs_test_csv> <hdfs_model_output_dir> "
                    + "<hdfs_task3_output_dir> <hdfs_prediction_output_dir>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "task5-credit-nb-prediction");
        job.setJarByClass(CreditRiskPredictJob.class);
        job.setMapperClass(PredictMapper.class);
        job.setReducerClass(PredictionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        CreditRiskFeatures.addCacheFile(job,
                new Path(args[1], "part-r-00000"), "credit-risk-model.tsv");
        CreditRiskFeatures.addCacheFile(job,
                new Path(args[2], "part-r-00000"), "credit-quartiles.tsv");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
