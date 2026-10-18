package com.example.hadoop;

/** Pure Java parsing and counting helpers; no Hadoop dependency. */
public final class StockLogic {
    private StockLogic() {}

    public static final class Record {
        public final String index;
        public final String ticker;

        public Record(String index, String ticker) {
            this.index = index;
            this.ticker = ticker;
        }
    }

    public static final class TaggedRecord {
        public final String index;
        public final String ticker;
        public final String tag;

        public TaggedRecord(String index, String ticker, String tag) {
            this.index = index;
            this.ticker = ticker;
            this.tag = tag;
        }
    }

    private static void validateIndex(String index) {
        if (!index.equals("101") && !index.equals("102") && !index.equals("103")) {
            throw new IllegalArgumentException("Invalid index (expected 101, 102, or 103): " + index);
        }
    }

    /** Returns null for an empty/whitespace-only line. */
    public static Record parseInput(String line) {
        String s = line.trim();
        if (s.isEmpty()) return null;
        String[] fields = s.split("\\s+");
        if (fields.length != 2) {
            throw new IllegalArgumentException("Expected two columns: " + line);
        }
        validateIndex(fields[0]);
        return new Record(fields[0], fields[1]);
    }

    /** Reads one line from the first MapReduce job. */
    public static TaggedRecord parseTagged(String line) {
        String s = line.trim();
        if (s.isEmpty()) return null;
        String[] fields = s.split("\\s+");
        if (fields.length != 3) {
            throw new IllegalArgumentException("Expected three columns: " + line);
        }
        validateIndex(fields[0]);
        if (!fields[2].equals("BOTH") && !fields[2].equals("A_ONLY")
                && !fields[2].equals("B_ONLY")) {
            throw new IllegalArgumentException("Invalid relation: " + fields[2]);
        }
        return new TaggedRecord(fields[0], fields[1], fields[2]);
    }

    public static String relation(boolean fromA, boolean fromB) {
        if (fromA && fromB) return "BOTH";
        if (fromA) return "A_ONLY";
        if (fromB) return "B_ONLY";
        throw new IllegalArgumentException("A record must occur in at least one file");
    }

    public static final class Stats {
        private int both;
        private int aOnly;
        private int bOnly;

        public void add(String tag) {
            switch (tag) {
                case "BOTH": both++; break;
                case "A_ONLY": aOnly++; break;
                case "B_ONLY": bOnly++; break;
                default: throw new IllegalArgumentException("Invalid relation: " + tag);
            }
        }

        public String asOutput() {
            return both + "\t" + aOnly + "\t" + bOnly + "\t" + (both + aOnly + bOnly);
        }
    }
}
