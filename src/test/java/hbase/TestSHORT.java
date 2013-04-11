package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Comparator;

import types.SHORT;
import util.HSerializer;

public class TestSHORT extends RandomTestHSerializable<Short> {

  protected Comparator<Short> getComparator() {
    return new Comparator<Short>() {
      @Override
      public int compare(Short o1, Short o2) {
        return o1.compareTo(o2);
      }
    };
  }

  protected Short create() {
    return Short.valueOf((short) r.nextInt());
  }

  protected HSerializer<Short> ascendingSerializer() { return new SHORT(ASCENDING); }
  protected HSerializer<Short> descendingSerializer() { return new SHORT(DESCENDING); }

}
