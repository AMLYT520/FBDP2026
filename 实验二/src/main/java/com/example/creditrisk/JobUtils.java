package com.example.creditrisk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

final class JobUtils {
    private JobUtils() {}

    static void cache(Job job, Path hdfsFile, String alias) throws IOException {
        FileSystem fs = hdfsFile.getFileSystem(job.getConfiguration());
        if (!fs.isFile(hdfsFile)) {
            throw new IOException("Required HDFS result is missing: " + hdfsFile);
        }
        URI original = fs.makeQualified(hdfsFile).toUri();
        job.addCacheFile(URI.create(original.toString() + "#" + alias));
    }

    static BufferedReader readCache(String alias) throws IOException {
        java.nio.file.Path local = Paths.get(alias);
        if (!Files.isRegularFile(local)) {
            throw new IOException("Distributed cache did not localize " + alias);
        }
        return Files.newBufferedReader(local, StandardCharsets.UTF_8);
    }

    static BufferedReader readHdfs(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.isFile(path)) {
            throw new IOException("Expected HDFS result is missing: " + path);
        }
        return new BufferedReader(
                new InputStreamReader(fs.open(path), StandardCharsets.UTF_8));
    }

    static void checkArgs(String[] args, int count, String usage) {
        if (args.length != count) {
            throw new IllegalArgumentException("Usage: " + usage);
        }
    }
}
