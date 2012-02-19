/** Altai State University, 2011 */
package edu.asu.hadoop.scaledata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.Point;
import edu.asu.hadoop.ConfigurationUtils;


/**
 * Scale Data (centring & normalizing) for Hadoop 0.21.0 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class ScaleMapper
    extends Mapper<LongWritable, Text, LongWritable, Text> {
    
    //TODO: Integrate DoubleArrayWriter
    
    private int dataDimFrom;
    private int dataDimTo;
    private double[] meanValues;
    private double[] stdevValues;
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        meanValues = ConfigurationUtils.getVector(conf, "meanValues");
        stdevValues = ConfigurationUtils.getVector(conf, "stdevValues");
    }
    
    @Override
    public void map(
            LongWritable key,
            Text row,
            Context context) throws IOException, InterruptedException {
        Point point = new Point(dataDimFrom, dataDimTo, row);
        for (int c = dataDimFrom; c <= dataDimTo; c++) {
            double val = (Double) point.get(c) - meanValues[c - dataDimFrom];
            if (stdevValues != null) {
                val /= stdevValues[c - dataDimFrom];
            }
            point.set(c, val);
        }
        context.write(key, point.toText());
    }
    
}
