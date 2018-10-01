package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen2 {

    // build a custom writable, first para is total distance (double), second para is number of edges (int)
    public static class Pair implements Writable {
        private Double distance;
        private Integer number;

        public Pair() {
        }

        public Pair(double d, int num) {
            set(d, num);
        }

        public void set(double d, int num) {
            distance = d;
            number = num;
        }

        public double getDis() {
            return distance;
        }

        public int getNum() {
            return number;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            distance =  in.readDouble();
            number = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(distance);
            out.writeInt(number);
        }
    }

    // in-mapper combining, calculate the edges and number of the same keys in mapper
    public static class AveMapper
            extends Mapper<Object, Text, IntWritable, Pair> {

        private HashMap<String, Pair> map;

        @Override
        public void setup(Context context
        ) throws IOException, InterruptedException {
             map = new HashMap<String, Pair>();
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] data = value.toString().split("\n");
            for (String datum : data) {
                String[] dString = datum.split(" ");
                if (!map.containsKey(dString[2])) {
                    map.put(dString[2], new Pair(Double.parseDouble(dString[3]), 1));
                } else {
                    double new_distance = map.get(dString[2]).getDis() + Double.parseDouble(dString[3]);
                    int new_number = map.get(dString[2]).getNum() + 1;
                    map.put(dString[2], new Pair(new_distance, new_number));
                }
            }
        }

        @Override
        public void cleanup(Context context
        ) throws IOException, InterruptedException {
            for (Map.Entry<String, Pair> m : map.entrySet()) {
                context.write(new IntWritable(Integer.parseInt(m.getKey())), m.getValue());
            }
        }

    }

    // same as reducer in EdgeAvgLen1.java
    public static class AveReducer
            extends Reducer<IntWritable, Pair, IntWritable, DoubleWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<Pair> values, Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int num = 0;
            for (Pair datum : values) {
                sum += datum.getDis();
                num += datum.getNum();
            }
            context.write(key, new DoubleWritable(sum/(double)num));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EdgeAvgLen2");
        job.setJarByClass(EdgeAvgLen2.class);
        job.setMapperClass(AveMapper.class);
        job.setReducerClass(AveReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Pair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}