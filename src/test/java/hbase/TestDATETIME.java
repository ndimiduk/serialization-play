package hbase;

import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Comparator;
import java.util.Date;

import types.DATETIME;
import util.HSerializer;

public class TestDATETIME extends RandomTestHSerializable<Date> {

  protected Comparator<Date> getComparator() {
    return new Comparator<Date>() {
      @Override
      public int compare(Date o1, Date o2) {
        return o1.compareTo(o2);
      }
    };
  }

  protected Date create() {
    return new Date(r.nextLong());
  }

  protected HSerializer<Date> ascendingSerializer() { return new DATETIME(ASCENDING); }
  protected HSerializer<Date> descendingSerializer() { return new DATETIME(DESCENDING); }

}
