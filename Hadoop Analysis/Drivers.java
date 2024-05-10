import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Drivers extends Configured implements Tool{

    static int printUsage() {
        System.out.println("Drivers [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class DriversMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text word = new Text();

        String taxiExpression = "^[0-9A-F]+$";
        Pattern taxiPattern = Pattern.compile(taxiExpression);

        String revenueExpression = "^\\d*\\.?\\d+$";
        Pattern revenuePattern = Pattern.compile(revenueExpression);


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // each key should be a date
            // each key should have a value as the Drivers
            // somehow calculate the Drivers
            // should be 11 total tokens in each line

            String[] tokens = value.toString().split(",");

            if(tokens.length == 11) {
                String medallion = tokens[1];
                String rev = tokens[10];

                Matcher matcher1 = taxiPattern.matcher(medallion);
                Matcher matcher2 = revenuePattern.matcher(rev);

                if (matcher1.matches() && matcher2.matches()) {
                    word.set(medallion);
                    double Drivers = Double.parseDouble(tokens[10]);
                    DoubleWritable currDrivers = new DoubleWritable(Drivers);
                    context.write(word, currDrivers);
                }
            }
        }
    }


    public static class DriversReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context
        ) throws IOException, InterruptedException {
            double totalDrivers = 0;
            for (DoubleWritable val : values) {
                totalDrivers += val.get();
            }
            result.set(totalDrivers);
            context.write(key, result);

        }
    }

    public static class Top5Mapper extends Mapper<Object, Text, Text, Text>{

        private TreeMap<Double, String> driverMap = new TreeMap<>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException{

            String[] tokens = value.toString().split("\t");
            String taxi = tokens[0];
            Double revenue = Double.parseDouble(tokens[1]);

            driverMap.put(revenue, taxi);

            if(driverMap.size() > 5){
                driverMap.remove(driverMap.firstKey());
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException{
            for(Map.Entry<Double, String> entry: driverMap.entrySet()){
                String tempKey = entry.getValue();
                Double tempVal = entry.getKey();
                String mixed_val = tempKey + "," + tempVal;
                context.write(new Text("tempKey"), new Text(mixed_val));
            }
        }
    }

    public static class Top5Reducer extends Reducer<Text, Text, Text, DoubleWritable>{

        private TreeMap<Double, String> revenueMap = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> value, Context context){
            for(Text val: value) {
                String[] key_val = val.toString().split(",");
                String taxi_name = key_val[0];
                String taxi_revenue = key_val[1];

                double revenue = Double.parseDouble(taxi_revenue);

                revenueMap.put(revenue, taxi_name);

                if(revenueMap.size() > 5){
                    revenueMap.remove(revenueMap.firstKey());
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException{

            for(Map.Entry<Double, String> entry: revenueMap.entrySet()){
                context.write(new Text(entry.getValue()), new DoubleWritable(entry.getKey()));
            }
        }


    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Drivers");
        job.setJarByClass(Drivers.class);
        job.setMapperClass(DriversMapper.class);
        job.setCombinerClass(DriversReducer.class);
        job.setReducerClass(DriversReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(job, other_args.get(0));
        Path taxi_rev = new Path("~/revenue_pairs");
        FileOutputFormat.setOutputPath(job, taxi_rev);

        Job top5 = Job.getInstance(conf, "Top5");

        if(job.waitForCompletion(true)){
            top5.setJarByClass(Drivers.class);
            top5.setMapperClass(Top5Mapper.class);
            top5.setReducerClass(Top5Reducer.class);
            top5.setMapOutputKeyClass(Text.class);
            top5.setMapOutputValueClass(Text.class);
            top5.setOutputKeyClass(Text.class);
            top5.setOutputValueClass(DoubleWritable.class);
            top5.setNumReduceTasks(1);
            FileInputFormat.setInputPaths(top5, taxi_rev);
            FileOutputFormat.setOutputPath(top5, new Path(other_args.get(1)));

        }

        return (top5.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Drivers(), args);
        System.exit(res);
    }
}