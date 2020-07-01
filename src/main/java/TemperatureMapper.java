import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    @Override
    protected void map(Object key, Text value, Context context) {
        String line = value.toString();
        try {
            String[] columns = line.split("\",\"");
            String location = columns[0].substring(1);
            String stringTemperature = columns[13].split(",")[0];
            if (!stringTemperature.equals("+9999") && !stringTemperature.equals("TMP")) {
                double temperature = Double.parseDouble(stringTemperature) / 10;
                context.write(new Text(location), new DoubleWritable(temperature));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
