package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;
import types.DOUBLE;
import util.HSerializer;

public class TestDOUBLE extends RandomTestHSerializable<Double> {

  protected Double create() {
    return r.nextDouble();
  }

  protected HSerializer<Double> ascendingSerializer() { return new DOUBLE(ASCENDING); }
  protected HSerializer<Double> descendingSerializer() { return new DOUBLE(DESCENDING); }

}
