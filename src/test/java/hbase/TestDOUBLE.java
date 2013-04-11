package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Comparator;

import types.DOUBLE;
import util.HSerializer;

public class TestDOUBLE extends RandomTestHSerializable<Double> {

  protected Comparator<Double> getComparator() {
    return new Comparator<Double>() {
      @Override
      public int compare(Double o1, Double o2) {
        return o1.compareTo(o2);
      }
    };
  }

  protected Double create() {
    return r.nextDouble();
  }

  protected HSerializer<Double> ascendingSerializer() { return new DOUBLE(ASCENDING); }
  protected HSerializer<Double> descendingSerializer() { return new DOUBLE(DESCENDING); }

}
