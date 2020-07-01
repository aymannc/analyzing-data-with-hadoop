import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double min = Double.MAX_VALUE;
        double max = -min;

        for (DoubleWritable val : values) {
            min = Double.min(min, val.get());
            max = Double.max(max, val.get());
        }
        context.write(key, new DoubleWritable(min));
        context.write(key, new DoubleWritable(max));
    }
}
