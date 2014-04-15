package pig.ml.reco.cf.sim;

import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import pig.ml.reco.cf.udf.SIM;

public class TestCosineSimilarity {

	@Test
	public void test() throws IOException {
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		Tuple bagTuple = TupleFactory.getInstance().newTuple(4);
		bagTuple.set(0, 30238576);
		bagTuple.set(2, 30298495);
		
		DataBag innerBag1 = BagFactory.getInstance().newDefaultBag();
		Tuple valueTuple = TupleFactory.getInstance().newTuple();
		valueTuple.set(0, 123);
		valueTuple.set(1, 30238576);
		valueTuple.set(2, 4);
		bagTuple.set(1, innerBag1);
		
		DataBag innerBag2 = BagFactory.getInstance().newDefaultBag();
		bagTuple.set(3, innerBag2);
		
		Tuple tuple = TupleFactory.getInstance().newTuple(3);
		tuple.set(0, bag);
		tuple.set(1, 2);
		tuple.set(2, "COSINE");
		
		SIM sim = new SIM();
		DataBag prodSimilarities = sim.exec(tuple);
		System.out.println(prodSimilarities);
	}

}
