package pig.ml.reco.cf.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pig.ml.reco.cf.sim.Similarity;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class SIM extends AccumulatorEvalFunc<DataBag> {

  private static final Logger logger = LoggerFactory.getLogger(SIM.class);
  private static final String PKGPREFIX = "pig.ml.reco.cf.sim.";

  private Similarity similarity;
  private int threshold;
  
  private DataBag prodSimilarities = BagFactory.getInstance().newDistinctBag();
  
  public SIM(String threshold, String similarityClass) throws ExecException {
    this.threshold = Integer.parseInt(threshold);

    Class<?> similarityKlass = null;
    try {
      similarityKlass = Class.forName(PKGPREFIX + (String) similarityClass);
      Preconditions.checkArgument(Similarity.class.isAssignableFrom(similarityKlass));
      similarity = (Similarity) similarityKlass.newInstance();
      logger.debug("Similarity Class configured is {} ", similarity.getClass());
    } catch (Exception e) {
      throw new ExecException(e.getMessage(), e);
    }
  }
  
  private DataBag getSimilarityBag(Tuple input) throws IOException {
    try {
      Object object = input.get(0);
      Preconditions.checkArgument(object instanceof DataBag);
      DataBag inputBag = (DataBag) object;

      Tuple tupleSimilarity = null;
      Multimap<Object, Number> userWeightMap = ArrayListMultimap.create();

      for (Tuple inputTuple : inputBag) {
        Preconditions.checkArgument(inputTuple.size() == 4);

        Object product1 = inputTuple.get(0);
        Object product2 = inputTuple.get(2);

        if (DataType.compare(product1, product2) == 0) {
          continue;
        }

        tupleSimilarity = TupleFactory.getInstance().newTuple(3);
        tupleSimilarity.set(0, product1);
        tupleSimilarity.set(1, product2);
        tupleSimilarity.set(2, 0.0d);

        Object dataBagObj1 = inputTuple.get(1);
        Preconditions.checkArgument(dataBagObj1 instanceof DataBag);
        DataBag bag1 = (DataBag) dataBagObj1;

        Object dataBagObj2 = inputTuple.get(3);
        Preconditions.checkArgument(dataBagObj2 instanceof DataBag);
        DataBag bag2 = (DataBag) dataBagObj2;

        for (Tuple t : bag1) {
          userWeightMap.put(t.get(0), (Number) t.get(2));
        }

        if (userWeightMap.keySet().size() < threshold) {
          userWeightMap.clear();
          continue;
        }

        for (Tuple t : bag2) {
          userWeightMap.put(t.get(0), (Number) t.get(2));
        }

        List<Number> weightsList1 = new ArrayList<Number>();
        List<Number> weightsList2 = new ArrayList<Number>();

        for (Entry<Object, Collection<Number>> entry : userWeightMap.asMap().entrySet()) {
          Collection<Number> weightsByUser = entry.getValue();
          if (weightsByUser.size() < 2) {
            continue;
          }
          Iterator<Number> valueItr = weightsByUser.iterator();
          weightsList1.add(valueItr.next());
          weightsList2.add(valueItr.next());
        }

        if (weightsList1.size() >= threshold) {
          tupleSimilarity.set(2, similarity.getSimilarity(weightsList1, weightsList2));
          prodSimilarities.add(tupleSimilarity);
        }
        userWeightMap.clear();
      }
      
      return prodSimilarities;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new ExecException(e.getMessage(), e);
    }
  }

  @Override
  public DataBag exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.isNull(0) == true) {
      throw new ExecException("Input is not provided.");
    }
    return getSimilarityBag(input);
  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema fieldSchema = new Schema();
    fieldSchema.add(new Schema.FieldSchema("item1", DataType.CHARARRAY));
    fieldSchema.add(new Schema.FieldSchema("item2", DataType.CHARARRAY));
    fieldSchema.add(new Schema.FieldSchema("similarity", DataType.DOUBLE));
    
    try {
      return new Schema(new Schema.FieldSchema("tuple", fieldSchema, DataType.TUPLE));
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
  
  DataBag intermediateBag = BagFactory.getInstance().newDistinctBag();

  @Override
  public void accumulate(Tuple input) throws IOException {
    try {
      Object object = input.get(0);
      Preconditions.checkArgument(object instanceof DataBag);
      DataBag inputBag = (DataBag) object;
      
      TupleFactory tf = TupleFactory.getInstance();
      Tuple newTuple = null;
      
      for (Tuple inputTuple : inputBag) {
        Preconditions.checkArgument(inputTuple.size() == 4);

        int compare = DataType.compare(inputTuple.get(0), inputTuple.get(2));
        if (compare == 0) {
          continue;
        } else if (compare > 0) {
          newTuple = tf.newTuple(4);
          newTuple.set(0, inputTuple.get(2));
          newTuple.set(1, inputTuple.get(3));
          newTuple.set(2, inputTuple.get(0));
          newTuple.set(3, inputTuple.get(1));
        } else {
          newTuple = tf.newTuple(inputTuple.getAll());
        }
        intermediateBag.add(newTuple);
      }
    } catch(Exception e) {
      logger.error(e.getMessage(), e);
      throw new ExecException(e.getMessage(), e);
    }
  }

  @Override
  public void cleanup() {
    intermediateBag.clear();
  }

  @Override
  public DataBag getValue() {
    try {
      Tuple tupleSimilarity = null;
      Multimap<Object, Number> userWeightMap = ArrayListMultimap.create();

      for (Tuple inputTuple : intermediateBag) {
        tupleSimilarity = TupleFactory.getInstance().newTuple(3);
        tupleSimilarity.set(0, inputTuple.get(0));
        tupleSimilarity.set(1, inputTuple.get(2));
        tupleSimilarity.set(2, 0.0d);

        Object dataBagObj1 = inputTuple.get(1);
        Preconditions.checkArgument(dataBagObj1 instanceof DataBag);
        DataBag bag1 = (DataBag) dataBagObj1;

        Object dataBagObj2 = inputTuple.get(3);
        Preconditions.checkArgument(dataBagObj2 instanceof DataBag);
        DataBag bag2 = (DataBag) dataBagObj2;

        for (Tuple t : bag1) {
          userWeightMap.put(t.get(0), (Number) t.get(2));
        }

        if (userWeightMap.keySet().size() < threshold) {
          userWeightMap.clear();
          continue;
        }

        for (Tuple t : bag2) {
          userWeightMap.put(t.get(0), (Number) t.get(2));
        }

        List<Number> weightsList1 = new ArrayList<Number>();
        List<Number> weightsList2 = new ArrayList<Number>();

        for (Entry<Object, Collection<Number>> entry : userWeightMap.asMap().entrySet()) {
          Collection<Number> weightsByUser = entry.getValue();
          if (weightsByUser.size() < 2) {
            continue;
          }
          Iterator<Number> valueItr = weightsByUser.iterator();
          weightsList1.add(valueItr.next());
          weightsList2.add(valueItr.next());
        }

        if (weightsList1.size() >= threshold) {
          tupleSimilarity.set(2, similarity.getSimilarity(weightsList1, weightsList2));
          prodSimilarities.add(tupleSimilarity);
        }
        userWeightMap.clear();
      }
      return prodSimilarities;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}

