package com.example.creditrisk;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared train.csv reader.
 * train.csv has 15 columns, with bad_good at index 2.
 * Invalid numeric values cause a row to be skipped; empty numeric fields mean zero.
 */
final class TrainRow {
    static final String[] CATEGORY_NAMES = {
        "存款", "理财", "私人理财", "基金", "国债", "银保通", "贵金属"
    };

    final String org;
    final int label;
    final BigDecimal[] categories; // CSV positions 3..9
    final BigDecimal asset;        // CSV position 10
    final BigDecimal debt;         // CSV position 11

    private TrainRow(String org, int label, BigDecimal[] categories,
                     BigDecimal asset, BigDecimal debt) {
        this.org = org;
        this.label = label;
        this.categories = categories;
        this.asset = asset;
        this.debt = debt;
    }

    static BigDecimal decimal(String s) {
        String t = s.trim();
        return t.isEmpty() || t.equalsIgnoreCase("null") || t.equalsIgnoreCase("nan")
                || t.equals("\\N") ? BigDecimal.ZERO : new BigDecimal(t);
    }

    static String money(BigDecimal x) {
        BigDecimal y = x.stripTrailingZeros();
        return y.signum() == 0 ? "0" : y.toPlainString();
    }

    static TrainRow parse(String raw) {
        String line = raw;
        if (line.startsWith("\uFEFF")) {
            line = line.substring(1);
        }
        if (line.isBlank() || line.startsWith("CUST_ID,")) {
            return null;
        }

        String[] f = csvFields(line);
        if (f.length != 15) {
            throw new IllegalArgumentException("Expected 15 columns, got " + f.length);
        }

        if (f[0].trim().isEmpty() || f[1].trim().isEmpty()) {
            throw new IllegalArgumentException("Missing customer ID or organization");
        }
        String labelText = f[2].trim();
        if (!labelText.equals("0") && !labelText.equals("1")) {
            throw new IllegalArgumentException("bad_good must be 0 or 1");
        }

        BigDecimal[] categories = new BigDecimal[7];
        for (int i = 0; i < 7; ++i) {
            categories[i] = decimal(f[i + 3]);
        }
        BigDecimal asset = decimal(f[10]);
        BigDecimal debt = decimal(f[11]);

        return new TrainRow(f[1].trim(), Integer.parseInt(labelText),
                categories, asset, debt);
    }

    // Supports quoted fields and doubled quotes in a single CSV record.
    // Newlines inside quoted fields are not supported by Hadoop TextInputFormat.
    static String[] csvFields(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); ++i) {
            char c = line.charAt(i);
            if (c == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cell.append('"');
                    ++i;
                } else {
                    quoted = !quoted;
                }
            } else if (c == ',' && !quoted) {
                fields.add(cell.toString());
                cell.setLength(0);
            } else {
                cell.append(c);
            }
        }
        if (quoted) {
            throw new IllegalArgumentException("Unterminated CSV quote");
        }
        fields.add(cell.toString());
        return fields.toArray(new String[0]);
    }
}
