/** Altai State University, 2011 */
package edu.asu.hadoop.pca.transformation;

/**
 *
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class PCATransformation {
    
    public static double[] vectorToPCA(double[] vector, double[][] PCAComponents, double[] meanValues, double[] stdevValues) {
        double[] outVector = new double[PCAComponents.length];
        for (int p = 0; p < PCAComponents.length; p++) {
            for (int i = 0; i < vector.length; i++) {
                double pointVal = 0;
                if (meanValues != null) {
                    pointVal = vector[i] - meanValues[i];
                    if (stdevValues != null) {
                        pointVal /= stdevValues[i];
                    }
                } else {
                    pointVal = vector[i];
                }
                outVector[p] += pointVal*PCAComponents[p][i];
            }
        }
        return outVector;
    }
    
}
