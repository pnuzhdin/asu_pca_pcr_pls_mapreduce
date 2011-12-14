/** Altai State University, 2011 */
package edu.asu.eigen;

import java.util.Random;
import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.EigenDecompositionImpl;
import org.apache.commons.math.linear.RealMatrix;

/**
 *
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class EigenTest {
    public static void main( String[] args ) {
        Random random = new Random(System.currentTimeMillis());
        
        for (int universeSize = 2; universeSize < 1500; universeSize++) {
            double time = 0;
            for (int t = 0; t < 3; t++) {
                RealMatrix realMatrix = new Array2DRowRealMatrix(universeSize, universeSize);
                for (int i = 0; i < universeSize; i++) { //Decompress symmetric compact matrix into Apache Common Math matrix
                    for (int j = 0; j < universeSize; j++) {
                        if (j < i) {
                            realMatrix.setEntry(i, j, realMatrix.getEntry(j, i));
                        } else {
                            realMatrix.setEntry(i, j, random.nextDouble() * 100 + 0.01);
                        }
                    }
                }
                long firsttime = System.currentTimeMillis();
                EigenDecompositionImpl decompositionImpl = new EigenDecompositionImpl(realMatrix, 0); //NOTE: Second parameter not used
                decompositionImpl.getV();
                decompositionImpl.getRealEigenvalues();
                time += ((double) System.currentTimeMillis() - firsttime) / 3;
            }
            System.out.println(universeSize + " " + time);
        }
    }
}
