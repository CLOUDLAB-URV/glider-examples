package org.example.benchmark.filebw;

import java.nio.ByteBuffer;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.memory.OffHeapBuffer;

public class BenchCrail {
  private static int bufferSize = 1024*1024;
  private static long totalBytes = 10737418240L; // 10 GiB
  private static long operations = totalBytes / bufferSize;

  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    String crailPath = "/test-file";

    // Write
    CrailFile crailFile = store.create(crailPath, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, false)
        .get().asFile();
    System.out.println(crailFile.getPath() + " " + crailFile.getFd());

    long time1 = System.currentTimeMillis();
    writeBytesToCrail(bufferSize, operations, crailFile);
    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) bufferSize * operations / 1024;
    double bits = (double) bufferSize * operations * 8;

    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / operations + " s");

    // Read

    time1 = System.currentTimeMillis();
    long read = readFileFromCrail(bufferSize, operations, crailFile);
    time2 = System.currentTimeMillis();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) read / 1024;
    bits = (double) read * 8;

    System.out.println();
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / operations + " s");

    try {
      store.delete(crailPath, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    store.close();
  }

  /**
   * Writes bytes to a crail file.
   */
  static void writeBytesToCrail(int size, long ops, CrailFile crailFile) {
    try {
      CrailBufferedOutputStream crailBufferedOutputStream = crailFile.getBufferedOutputStream(size * ops);

      ByteBuffer buffer = ByteBuffer.allocate(size);

      for (int i = 0; i < ops; i++) {
        crailBufferedOutputStream.write(buffer);
        buffer.clear();

      }
      crailBufferedOutputStream.close();
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
    }
  }
  static void writeBytesToCrailDirect(int size, long ops, CrailFile crailFile) {
    try {
      CrailOutputStream crailOutputStream = crailFile.getDirectOutputStream(size * ops);

      CrailBuffer buffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));

      for (int i = 0; i < ops; i++) {
        crailOutputStream.write(buffer).get();
        buffer.clear();
      }
      crailOutputStream.close();
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
    }
  }

  /**
   * Reads a file from Crail servers.
   *
   */
  static long readFileFromCrail(int size, long ops, CrailFile crailFile) {
    long totalRead = 0;
    try {
      CrailBufferedInputStream crailBufferedInputStream = crailFile.getBufferedInputStream(size * ops);

      ByteBuffer buffer = ByteBuffer.allocate(size);

      int read;
      for (int i = 0; i < ops; i++) {
        buffer.clear();
        read = crailBufferedInputStream.read(buffer);
        if (read > 0) {
          totalRead += read;
        }
      }
      crailBufferedInputStream.close();
      return totalRead;
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
      return 0;
    }
  }

  static long readFileFromCrailDirect(int size, long ops, CrailFile crailFile) {
    long totalRead = 0;
    try {
      CrailInputStream crailInputStream = crailFile.getDirectInputStream(size * ops);

      CrailBuffer buffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));

      int read;
      for (int i = 0; i < ops; i++) {
        buffer.clear();
        read = (int) crailInputStream.read(buffer).get().getLen();
        if (read > 0) {
          totalRead += read;
        }
      }
      crailInputStream.close();
      return totalRead;
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
      return 0;
    }
  }
}
