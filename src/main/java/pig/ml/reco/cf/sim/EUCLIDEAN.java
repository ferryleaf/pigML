package pig.ml.reco.cf.sim;

import java.util.List;

public class EUCLIDEAN implements Similarity {

  public double getSimilarity(List<Number> weights1, List<Number> weights2) {
    double diffSum = 0;
    for (int i = 0; i < weights1.size(); ++i) {
      double diff = weights2.get(i).doubleValue() - weights1.get(i).doubleValue();
      diffSum += (diff * diff);
    }
    return Math.sqrt(diffSum);
  }
}
