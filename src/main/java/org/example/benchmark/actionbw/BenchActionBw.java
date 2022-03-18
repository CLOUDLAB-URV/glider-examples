package org.example.benchmark.actionbw;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveReadableChannel;
import org.apache.crail.core.ActiveWritableChannel;

public class BenchActionBw {
  private static long mbToWrite = 50 * 1024;

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
    proxy.create(BwAction.class);

    // Write    
    ActiveWritableChannel writableChannel = proxy.getWritableChannel();
    byte[] b = new byte[1024 * 1024];
    Random random = new Random();
    random.nextBytes(b);
    ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
    buffer.put(b).clear();

    long time1 = System.currentTimeMillis();
    
    for (long writen = 0; writen < mbToWrite; writen++) {
      writableChannel.write(buffer);
      buffer.clear();
    }
    writableChannel.close();

    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) mbToWrite * 1024;

    System.out.println("WRITE");
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " KiB");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " KiB/s");

    // Read
    buffer.clear();
    ActiveReadableChannel readableChannel = proxy.getReadableChannel();

    time1 = System.currentTimeMillis();

    long totalRead = 0;
    int currentRead = readableChannel.read(buffer);
    while (currentRead != -1) {
      totalRead += currentRead;
      buffer.clear();
      currentRead = readableChannel.read(buffer);
    }
    readableChannel.close();

    time2 = System.currentTimeMillis();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) totalRead / 1024;

    System.out.println("READ");
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " KiB");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " KiB/s");

    proxy.delete();
    store.close();
  }
}
