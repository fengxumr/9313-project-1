package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

public class EdgeAvgLen1 {

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

    // separate rows by "\n", separate numbers in each row by " ", return Text and Pair
    public static class AveMapper
            extends Mapper<Object, Text, IntWritable, Pair> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] data = value.toString().split("\n");
            for (String datum : data) {
                String[] dString = datum.split(" ");
                context.write(new IntWritable(Integer.parseInt(dString[2])), new Pair(Double.parseDouble(dString[3]), 1));
            }
        }
    }

    // combine the para of the same keys, same as reducer
    public static class AveCombiner
            extends Reducer<IntWritable, Pair, IntWritable, Pair> {

        @Override
        public void reduce(IntWritable key, Iterable<Pair> value, Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int num = 0;
            for (Pair datum : value) {
                sum += datum.getDis();
                num += datum.getNum();
            }
            context.write(key, new Pair(sum, num));
        }
    }

    // same as combiner
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
        Job job = Job.getInstance(conf, "EdgeAvgLen1");
        job.setJarByClass(EdgeAvgLen1.class);
        job.setMapperClass(AveMapper.class);
        job.setCombinerClass(AveCombiner.class);
        job.setReducerClass(AveReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Pair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}