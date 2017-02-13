/**
 * Created by nkkarpov on 2/9/17.
 */

import java.io.*;
import java.net.URI;

import hex.genmodel.GenModel;
import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.RowData;

import hex.genmodel.easy.prediction.RegressionModelPrediction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MrPojo {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Configuration c = new Configuration();

        String[] argv = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(argv[0]);
        Path output = new Path(argv[1]);

        Job j = Job.getInstance(c, "MrPojo");

        j.setJarByClass(MrPojo.class);
        j.setMapperClass(MapScore.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.addCacheFile(new URI("/user/amy/grid_3a5a7f36_9782_440f_9616_2da59102399f_model_0.zip"));

        j.waitForCompletion(true);
        System.out.println("Total time: " + (System.currentTimeMillis() - start));

        System.exit(0);
    }

    public static class MapScore extends Mapper<LongWritable, Text, Text, Text> {
        EasyPredictModelWrapper[] models = new EasyPredictModelWrapper[10];
        long startTime;

        @Override
        protected void setup(Context c) {
            System.out.println("Starting map");
            startTime = System.currentTimeMillis();
            try {
                for(int i = 0; i < 10; i++) {
                    models[i] = new EasyPredictModelWrapper(MojoModel.load("./grid_3a5a7f36_9782_440f_9616_2da59102399f_model_0.zip"));
                }
            } catch (Exception e) {
                System.out.println("Ooops");
                System.out.println(e);
            }
            System.out.println("Loaded models in: " + (System.currentTimeMillis() - startTime) % 1000);
        }

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String _line = value.toString();
            String[] _raw_features = _line.split(",");

            RowData row = new RowData();
            for (int i = 1; i < 301; i++) {
                row.put("C"+Integer.toString(i),_raw_features[i]);
            }

            StringBuilder val = new StringBuilder();
            try {
                for(int i = 0; i < 10; i++) {
                    RegressionModelPrediction p = (RegressionModelPrediction) models[i].predict(row);
                    if(i == 9) // if last element don't add comma
                        val.append(Double.toString(p.value));
                    else
                        val.append(Double.toString(p.value) + ",");
                }
                con.write(new Text(_line), new Text(val.toString()));
            } catch(PredictException pe) {}
        }

        @Override
        protected void cleanup(Context con) throws IOException, InterruptedException {
            System.out.println("Total mapper time: " + (System.currentTimeMillis() - startTime));
            System.out.println("End map");
        }
    }
}
