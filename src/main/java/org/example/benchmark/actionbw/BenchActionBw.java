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
  private static int bufferSize = 1024 * 1024;
  private static long totalBytes = 10737418240L; // 10 GiB
  private static long operations = totalBytes / bufferSize;

  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    String filename = "/bench-action";

    // Create
    CrailObject obj = store.create(filename, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, false)
        .get().asObject();
    CrailObjectProxy proxy = obj.getProxy();
    proxy.create(BwAction.class);

    // Write
    ActiveWritableChannel writableChannel = proxy.getWritableChannel();

    ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);

    long time1 = System.currentTimeMillis();

    for (long i = 0; i < operations; i++) {
      writableChannel.write(buffer);
      buffer.clear();
    }
    writableChannel.close();

    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) bufferSize * operations / 1024;
    double bits = (double) bufferSize * operations * 8;

    System.out.println("WRITE");
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / operations + " s");

    // Read
    ActiveReadableChannel readableChannel = proxy.getReadableChannel();

    time1 = System.currentTimeMillis();

    long totalRead = 0;
    int currentRead;
    for (int i = 0; i < operations; i++) {
      buffer.clear();
      currentRead = readableChannel.read(buffer);
      if (currentRead > 0) {
        totalRead += currentRead;
      }
    }
    readableChannel.close();

    time2 = System.currentTimeMillis();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) totalRead / 1024;
    bits = (double) totalRead * 8;

    System.out.println();
    System.out.println("READ");
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / operations + " s");

    proxy.delete();
    try {
      store.delete(filename, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
    store.close();
  }
}