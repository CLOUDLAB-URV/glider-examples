package org.example.benchmark.filebw;

import java.nio.ByteBuffer;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveReadableChannel;
import org.apache.crail.core.ActiveWritableChannel;

public class BenchActionCrail {
  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    String filename = "/bench-action";

    try {
      store.delete(filename, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Create
    CrailObject obj = store.create(filename, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, false)
        .get().asObject();
    CrailObjectProxy proxy = obj.getProxy();
    proxy.create(ActionBench.class);

    // Write
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(0);
    ActiveWritableChannel writableChannel = proxy.getWritableChannel();
    buffer.flip();
    writableChannel.write(buffer);
    writableChannel.close();

    // Read
    buffer.clear();
    ActiveReadableChannel readableChannel = proxy.getReadableChannel();
    while (readableChannel.read(buffer) != -1) {
      buffer.clear();
    }
    readableChannel.close();

    proxy.delete();
    store.close();
  }

}
