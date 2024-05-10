import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Revenue extends Configured implements Tool{

    static int printUsage() {
        System.out.println("revenue [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class RevenueMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text word = new Text();
        
        String dateExpression = "^\\d{4}-\\d{2}-\\d{2}$";
        Pattern datePattern = Pattern.compile(dateExpression);

        String revenueExpression = "^\\d*\\.?\\d+$";
        Pattern revenuePattern = Pattern.compile(revenueExpression);


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // each key should be a date
            // each key should have a value as the revenue
            // somehow calculate the revenue
            // should be 11 total tokens in each line

            String[] tokens = value.toString().split(",");

            if(tokens.length == 11) {
                String date = tokens[3].substring(0, 10);
                String revenueStr = tokens[10];

                Matcher matcher1 = datePattern.matcher(date);
                Matcher matcher2 = revenuePattern.matcher(revenueStr);

                if (matcher1.matches() && matcher2.matches()) {
                    word.set(date);
                    double revenue = Double.parseDouble(tokens[10]);
                    DoubleWritable currRevenue = new DoubleWritable(revenue);
                    context.write(word, currRevenue);
                }
            }
        }
    }


    public static class RevenueReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context
        ) throws IOException, InterruptedException {
            double totalRevenue = 0;
            for (DoubleWritable val : values) {
                totalRevenue += val.get();
            }

            result.set(totalRevenue);
            context.write(key, result);

        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "total revenue");
        job.setJarByClass(Revenue.class);
        job.setMapperClass(RevenueMapper.class);
        job.setCombinerClass(RevenueReducer.class);
        job.setReducerClass(RevenueReducer.class);
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
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Revenue(), args);
        System.exit(res);
    }



}