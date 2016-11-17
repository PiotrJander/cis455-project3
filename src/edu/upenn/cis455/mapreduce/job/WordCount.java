package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class WordCount implements Job {

    public void map(String key, String value, Context context) {

        context.write(value, "1");
    }

    public void reduce(String key, Iterator<String> values, Context context) {

        Iterable<String> iterable = () -> values;
        Stream<String> stream = StreamSupport.stream(iterable.spliterator(), false);
        int sum = stream.mapToInt(Integer::parseInt).sum();
        context.write(key, String.valueOf(sum));
    }

}
