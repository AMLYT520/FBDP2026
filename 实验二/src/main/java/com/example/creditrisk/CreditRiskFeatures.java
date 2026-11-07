package com.example.creditrisk;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * Shared feature engineering for both train.csv (15 columns) and test.csv
 * (14 columns, without bad_good).
 *
 * Feature indices and buckets must be identical in training and inference.
 */
public final class CreditRiskFeatures {
    private CreditRiskFeatures() {}

    public static final int FEATURES = 14;

    public static final class Row {
        public final String customerId;
        public final int label;  // -1 for test
        public final String[] features;

        Row(String customerId, int label, String[] features) {
            this.customerId = customerId;
            this.label = label;
            this.features = features;
        }
    }

    public static final class Quartiles {
        private final BigDecimal q25, q50, q75;

        Quartiles(BigDecimal q25, BigDecimal q50, BigDecimal q75) {
            if (q25.compareTo(q50) > 0 || q50.compareTo(q75) > 0) {
                throw new IllegalArgumentException("Nonmonotonic quartiles");
            }
            this.q25 = q25;
            this.q50 = q50;
            this.q75 = q75;
        }

        public String tier(BigDecimal x) {
            if (x.compareTo(q25) < 0) return "T0";
            if (x.compareTo(q50) < 0) return "T1";
            if (x.compareTo(q75) < 0) return "T2";
            return "T3";
        }

        public static Quartiles load(String alias) throws IOException {
            try (BufferedReader br = localReader(alias)) {
                String line = br.readLine();
                if (line == null) {
                    throw new IOException("Quartiles cache is empty: " + alias);
                }
                String[] parts = line.trim().split("\\t", -1);
                if (parts.length != 3
                        || !parts[0].startsWith("Q25:")
                        || !parts[1].startsWith("Q50:")
                        || !parts[2].startsWith("Q75:")) {
                    throw new IOException("Invalid quartiles line: " + line);
                }
                try {
                    return new Quartiles(
                            new BigDecimal(parts[0].substring(4)),
                            new BigDecimal(parts[1].substring(4)),
                            new BigDecimal(parts[2].substring(4))
                    );
                } catch (NumberFormatException e) {
                    throw new IOException("Cannot parse quartiles", e);
                }
            }
        }
    }

    public static void addCacheFile(Job job, Path file, String alias)
            throws IOException {
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        if (!fs.isFile(file)) {
            throw new IOException("HDFS cache input missing: " + file);
        }
        URI qualified = fs.makeQualified(file).toUri();
        job.addCacheFile(URI.create(qualified.toString() + "#" + alias));
    }

    public static BufferedReader localReader(String alias) throws IOException {
        java.nio.file.Path file = Paths.get(alias);
        if (!Files.isRegularFile(file)) {
            throw new IOException("Hadoop distributed cache missing local file: " + alias);
        }
        return Files.newBufferedReader(file, StandardCharsets.UTF_8);
    }

    /** Parse one CSV record (no embedded line breaks inside quoted strings). */
    static String[] splitCsv(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder buffer = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); ++i) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    buffer.append('"');
                    ++i;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                fields.add(buffer.toString());
                buffer.setLength(0);
            } else {
                buffer.append(ch);
            }
        }
        if (inQuotes) {
            throw new IllegalArgumentException("Unclosed CSV quote");
        }
        fields.add(buffer.toString());
        return fields.toArray(new String[0]);
    }

    private static boolean nullish(String val) {
        String t = val.trim();
        return t.isEmpty() || t.equalsIgnoreCase("null")
                || t.equalsIgnoreCase("nan") || t.equalsIgnoreCase("na")
                || t.equals("\\N");
    }

    private static BigDecimal number(String val) {
        return nullish(val) ? BigDecimal.ZERO : new BigDecimal(val.trim());
    }

    // Amounts are discretized to fixed, human-readable value bands:
    // ZERO, NEG, P0:(0,100), P1:[100,1000), ..., P8:[1e9,+inf)
    private static final BigDecimal[] THRESHOLDS = {
        new BigDecimal("100"),
        new BigDecimal("1000"),
        new BigDecimal("10000"),
        new BigDecimal("100000"),
        new BigDecimal("1000000"),
        new BigDecimal("10000000"),
        new BigDecimal("100000000"),
        new BigDecimal("1000000000")
    };

    private static String bin(BigDecimal amount) {
        if (amount.signum() == 0) return "ZERO";
        if (amount.signum() < 0) return "NEG";
        for (int i = 0; i < THRESHOLDS.length; ++i) {
            if (amount.compareTo(THRESHOLDS[i]) < 0) return "P" + i;
        }
        return "P" + THRESHOLDS.length;
    }

    private static String flag(String val) {
        String t = val.trim().toUpperCase(Locale.ROOT);
        return t.equals("Y") || t.equals("N") ? t : "UNKNOWN";
    }

    private static String institution(String val) {
        String t = val.trim();
        return t.matches("[0-9]+") ? t : "UNKNOWN";
    }

    public static Row parse(String source, boolean training, Quartiles q) {
        String line = source;
        if (line.startsWith("\uFEFF")) line = line.substring(1);
        if (line.isBlank() || line.startsWith("CUST_ID,")) return null;

        String[] f = splitCsv(line);
        int expected = training ? 15 : 14;
        if (f.length != expected) {
            throw new IllegalArgumentException("Expected " + expected
                    + " fields, received " + f.length);
        }
        String customerId = f[0].trim();
        if (!customerId.matches("[0-9]+")) {
            throw new IllegalArgumentException("Invalid CUST_ID: " + customerId);
        }

        int label = -1;
        if (training) {
            String rawLabel = f[2].trim();
            if (!rawLabel.equals("0") && !rawLabel.equals("1")) {
                throw new IllegalArgumentException("Invalid bad_good label: " + rawLabel);
            }
            label = Integer.parseInt(rawLabel);
        }

        // In test.csv, all columns after OPEN_ORG_NUM shift left by one.
        int shift = training ? 0 : 1;

        BigDecimal saving = number(f[3 - shift]);
        BigDecimal fina = number(f[4 - shift]);
        BigDecimal privateFina = number(f[5 - shift]);
        BigDecimal fund = number(f[6 - shift]);
        BigDecimal govBond = number(f[7 - shift]);
        BigDecimal insurance = number(f[8 - shift]);
        BigDecimal metal = number(f[9 - shift]);
        BigDecimal asset = number(f[10 - shift]);
        BigDecimal debt = number(f[11 - shift]);
        BigDecimal loanBalance = number(f[12 - shift]);

        String[] x = new String[FEATURES];
        x[0]  = "ORG_" + institution(f[1]);
        x[1]  = "TIER_" + q.tier(asset);  // from Task3 quartiles
        x[2]  = bin(asset);
        x[3]  = bin(debt);
        x[4]  = bin(loanBalance);
        x[5]  = bin(saving);
        x[6]  = bin(fina);
        x[7]  = bin(privateFina);
        x[8]  = bin(fund);
        x[9]  = bin(govBond);
        x[10] = bin(insurance);
        x[11] = bin(metal);
        x[12] = "LOAN_" + flag(f[13 - shift]);
        x[13] = "CRED_" + flag(f[14 - shift]);

        return new Row(customerId, label, x);
    }
}
