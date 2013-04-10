package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import types.FLOAT;
import util.HSerializer;

public class TestFLOAT extends RandomTestHSerializable<Float> {

  protected Float create() {
    return r.nextFloat();
  }

  protected HSerializer<Float> ascendingSerializer() { return new FLOAT(ASCENDING); }
  protected HSerializer<Float> descendingSerializer() { return new FLOAT(DESCENDING); }

}
