package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Comparator;

import types.FLOAT;
import util.HSerializer;

public class TestFLOAT extends RandomTestHSerializable<Float> {

  protected Comparator<Float> getComparator() {
    return new Comparator<Float>() {
      @Override
      public int compare(Float o1, Float o2) {
        return o1.compareTo(o2);
      }
    };
  }

  protected Float create() {
    return r.nextFloat();
  }

  protected HSerializer<Float> ascendingSerializer() { return new FLOAT(ASCENDING); }
  protected HSerializer<Float> descendingSerializer() { return new FLOAT(DESCENDING); }

}
