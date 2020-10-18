package com.netbase.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;

import static jdk.nashorn.internal.objects.NativeMath.round;


public class App 
{
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);
        Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdownNow));

        File[] files;
        try {
            files = new File(args[0]).listFiles();
        } catch (Exception e) {
            System.out.println("Please provide a valid directory!");
            return;
        }

        Map<String, LongAdder> outputCounts = new ConcurrentHashMap<>();
        List<Future<Void>> futureList = new ArrayList<>();
        for (File fileEntry : Objects.requireNonNull(files)) {
            if (fileEntry.isDirectory()) {
                continue;
            }

            Future<Void> future = executor.submit(() -> {
                calculateBigramProbability(fileEntry, outputCounts);
                return null;
            });
            futureList.add(future);
        }
        for (Future<Void> future: futureList) {
            future.get();
        }
        executor.shutdown();
    }

    private static void calculateBigramProbability(File file, final Map<String, LongAdder> outputCounts) throws FileNotFoundException {
        Map<String, Integer> bigram_count = new HashMap<>();
        String regex = "[a-zA-Z0-9_]*";
        Scanner sc = new Scanner(file).useDelimiter(regex);

        String prevWordLower = null;
        while (sc.hasNext()) {
            String currWordLower = sc.next().toLowerCase();
            if (prevWordLower != null) {
                String bigram = prevWordLower + ' ' + currWordLower;
                int count = bigram_count.getOrDefault(bigram, 0);
                bigram_count.put(bigram, count + 1);
            }
            prevWordLower = currWordLower;
        }

        int product = 1;
        int k = 0;
        for (int count : bigram_count.values()) {
            product *= count;
            k += count;
        }
        if (k == 0) {
            System.out.println("No valid words in file: " + file.getName());
            return;
        }

        double probability = round(Math.pow(product, 1.0/k), 5);
        String outputKey = k + ", " + probability;
        outputCounts.compute(outputKey, (key, value) -> {
            if (value == null) {
                System.out.println(key + ", 1");
                return new LongAdder();
            }
            System.out.println(key + ", " + (value.sum() + 1));
            return value;
        }).increment();
    }
}
