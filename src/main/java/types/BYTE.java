package types;

import java.nio.ByteBuffer;

import util.HSerializer;

public class BYTE extends HSerializer<byte[]> {

  public BYTE() { super(); }
  public BYTE(Order order) { super(order); }

  @Override
  public boolean supportsNull() {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public byte[] toBytes(byte[] val) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public void write(ByteBuffer buff, byte[] val) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public byte[] fromBytes(byte[] bytes) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public byte[] read(ByteBuffer buff) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

}
