package pig.ml.reco.cf.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.BackendException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pig.ml.reco.cf.sim.Similarity;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class SIM extends EvalFunc<DataBag> {

  private static final Logger logger = LoggerFactory.getLogger(SIM.class);
  private static final String PKGPREFIX = "pig.ml.reco.cf.sim.";
  private Similarity similarity;

  @Override
  public DataBag exec(Tuple input) throws IOException {
    try {
      Object object = input.get(0);
      Preconditions.checkArgument(object instanceof DataBag);
      DataBag inputBag = (DataBag) object;

      Object thresHold = input.get(1);
      Preconditions.checkArgument(thresHold instanceof Integer);
      Integer threshold = (Integer) thresHold;

      Object similarityClass = input.get(2);
      Preconditions.checkArgument(similarityClass instanceof String);
      Class<?> similarityKlass = null;
      try {
        similarityKlass = Class.forName(PKGPREFIX + (String) similarityClass);
      } catch (ClassNotFoundException e) {
        logger.error(e.getMessage(), e);
        throw new BackendException(e.getMessage(), e);
      }

      Preconditions.checkArgument(Similarity.class.isAssignableFrom(similarityKlass));
      try {
        similarity = (Similarity) similarityKlass.newInstance();
        logger.debug("Similarity Class configured is {} ", similarity.getClass());
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        throw new BackendException(e.getMessage(), e);
      }

      DataBag prodSimilarities = BagFactory.getInstance().newDefaultBag();
      Tuple tupleSimilarity = null;
      Multimap<Object, Number> userWeightMap = ArrayListMultimap.create();

      for (Tuple inputTuple : inputBag) {
        Preconditions.checkArgument(inputTuple.size() == 4);

        Object product1 = inputTuple.get(0);
        Object product2 = inputTuple.get(2);

        if (product1.equals(product2)) {
          continue;
        }

        tupleSimilarity = TupleFactory.getInstance().newTuple(3);
        tupleSimilarity.set(0, product1);
        tupleSimilarity.set(1, product2);

        Object dataBagObj1 = inputTuple.get(1);
        Preconditions.checkArgument(dataBagObj1 instanceof DataBag);

        Object dataBagObj2 = inputTuple.get(3);
        Preconditions.checkArgument(dataBagObj2 instanceof DataBag);

        DataBag bag1 = (DataBag) dataBagObj1;
        for (Tuple t : bag1) {
          userWeightMap.put(t.get(0), (Number) t.get(2));
        }

        DataBag bag2 = (DataBag) dataBagObj2;
        for (Tuple t : bag2) {
          userWeightMap.put(t.get(0), (Number) t.get(2));
        }

        if (userWeightMap.keySet().size() >= threshold) {
          List<Number> weightsList1 = new ArrayList<Number>();
          List<Number> weightsList2 = new ArrayList<Number>();

          Map<Object, Collection<Number>> map = userWeightMap.asMap();
          Set<Entry<Object, Collection<Number>>> entries = map.entrySet();
          for (Entry<Object, Collection<Number>> entry : entries) {
            Collection<Number> weightsByUser = entry.getValue();
            if (weightsByUser.size() < 2) {
              continue;
            }
            Iterator<Number> valueItr = entry.getValue().iterator();
            weightsList1.add(valueItr.next());
            weightsList2.add(valueItr.next());
          }

          Preconditions.checkArgument(weightsList1.size() == weightsList2.size());
          if (weightsList1.size() >= threshold) {
            tupleSimilarity.set(2, similarity.getSimilarity(weightsList1, weightsList2));
          }
        }
        userWeightMap.clear();
        prodSimilarities.add(tupleSimilarity);
      }
      return prodSimilarities;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new BackendException(e.getMessage(), e);
    }
  }
}
