package pig.ml.reco.cf.sim;

import java.util.List;

public class COSINE implements Similarity {

  private double sqrtOfsquares(List<Number> weights) {
    double product = 1d;
    for (Number w : weights) {
      product *= (w.doubleValue() * w.doubleValue());
    }
    return Math.sqrt(product);
  }

  public double getSimilarity(List<Number> weights1, List<Number> weights2) {
    double product = 1d;
    for (int i = 0; i < weights1.size(); ++i) {
      product *= (weights1.get(i).doubleValue() * weights2.get(i).doubleValue());
    }
    return product / (sqrtOfsquares(weights1) * sqrtOfsquares(weights2));
  }
}
