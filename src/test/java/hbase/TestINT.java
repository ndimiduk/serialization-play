package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Comparator;

import types.INT;
import util.HSerializer;

public class TestINT extends RandomTestHSerializable<Integer> {

  protected Comparator<Integer> getComparator() {
    return new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o1.compareTo(o2);
      }
    };
  }

  protected Integer create() {
    return r.nextInt();
  }

  protected HSerializer<Integer> ascendingSerializer() { return new INT(ASCENDING); }
  protected HSerializer<Integer> descendingSerializer() { return new INT(DESCENDING); }

}
