import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AvgTempCities {

    private static final String OUTPUT_PATH = "/avg_temp_cities/output";

    public static class CoordinateMapper extends Mapper<Object, Text, Text, Text> {

        private Text filename = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            filename.set(filePath.substring(filePath.lastIndexOf('/') + 1));
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (value.toString().contains("Date,Longitude,Latitude,Model,Scenario,Variable,Value")) {
                return; // header of csv file
            }

            String[] line = value.toString().split(",");

            context.write(new Text(filename + "," + line[0] + "," + line[5] + ","), new Text(line[6]));
            // ex: atlanta,1950-01-01,tasmax, 289.12312
        }
    }

    public static class AvgCoordinatesReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Double sum = 0d;
            int total = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                total++;
            }

            Double avg = sum / total;
            context.write(key, new Text(avg.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(AvgTempCities.class);
        job.setJar("AvgTempCities.jar");
        job.setMapperClass(CoordinateMapper.class);
        job.setCombinerClass(AvgCoordinatesReducer.class);
        job.setReducerClass(AvgCoordinatesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


