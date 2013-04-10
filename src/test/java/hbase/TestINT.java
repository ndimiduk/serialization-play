package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import types.INT;
import util.HSerializer;

public class TestINT extends RandomTestHSerializable<Integer> {

  protected Integer create() {
    return r.nextInt();
  }

  protected HSerializer<Integer> ascendingSerializer() { return new INT(ASCENDING); }
  protected HSerializer<Integer> descendingSerializer() { return new INT(DESCENDING); }

}
