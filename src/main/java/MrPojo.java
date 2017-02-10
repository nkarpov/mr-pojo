/**
 * Created by nkkarpov on 2/9/17.
 */

import java.io.IOException;

import hex.genmodel.GenModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.RowData;

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
        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = new Job(c, "MrPojo");

        j.setJarByClass(MrPojo.class);
        j.setMapperClass(MapScore.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0:1);
    }

    public static class MapScore extends Mapper<LongWritable, Text, Text, Text> {
        GenModel raw_model = new GBM_model_R_1486751926267_1();
        EasyPredictModelWrapper model = new EasyPredictModelWrapper(raw_model);

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            GenModel raw_model = new GBM_model_R_1486751926267_1();
            EasyPredictModelWrapper model = new EasyPredictModelWrapper(raw_model);

            String _line = value.toString();
            String[] _raw_features = _line.split(",");

            RowData row = new RowData();
            for (int i = 0; i < 4; i++) {
                row.put(raw_model.getNames()[i],_raw_features[i]);
            }

            MultinomialModelPrediction p;
            try {
                p = (MultinomialModelPrediction) model.predict(row);
                con.write(new Text(_line), new Text(p.label));
            } catch(PredictException pe) {}
        }
    }

}
