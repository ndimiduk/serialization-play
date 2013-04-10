package types;


import java.math.BigDecimal;
import java.nio.ByteBuffer;

import util.HSerializer;

public class DECIMAL extends HSerializer<BigDecimal> {

  public DECIMAL() { super(); }
  public DECIMAL(Order order) { super(order); }

  @Override
  public boolean supportsNull() {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public byte[] toBytes(BigDecimal val) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public void write(ByteBuffer buff, BigDecimal val) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public BigDecimal fromBytes(byte[] bytes) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public BigDecimal read(ByteBuffer buff) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

}
