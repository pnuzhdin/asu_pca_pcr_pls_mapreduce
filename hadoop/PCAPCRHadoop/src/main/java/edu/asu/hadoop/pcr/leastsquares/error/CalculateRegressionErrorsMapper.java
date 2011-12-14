/** Altai State University, 2011 */
package edu.asu.hadoop.pcr.leastsquares.error;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.DoubleArrayWritable;


/**
 * Calculate regression errors (PCR, MLP, least squares) for Hadoop 0.21.0 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateRegressionErrorsMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, VLongWritable, DoubleWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private int dependentVariable;
    private long samplesCount;
    private double[] regressionParameter;
    
    // hint
    private DoubleWritable outVal = new DoubleWritable();
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        dependentVariable = conf.getInt("dependentVariable", 0);
        samplesCount = conf.getLong("samplesCount", 0);
        regressionParameter = ConfigurationUtils.getVector(conf, "regressionParameter");
    }
    
    @Override
    public void map(
            VLongWritable key,
            DoubleArrayWritable array,
            Context context) throws IOException, InterruptedException {
        double response = 0;
        for (int i = dataDimFrom; i <= dataDimTo; i++) {
            response += array.get(i).get() * regressionParameter[i - dataDimFrom];
        }
        response += regressionParameter[dataDimTo - dataDimFrom + 1];
        outVal.set(response - array.get(dependentVariable).get());
        context.write(key, outVal);
    }
    
}
