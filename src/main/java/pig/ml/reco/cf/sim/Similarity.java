package pig.ml.reco.cf.sim;

import java.util.List;

public interface Similarity {

  public abstract double getSimilarity(List<Number> weights1, List<Number> weights2);

}
