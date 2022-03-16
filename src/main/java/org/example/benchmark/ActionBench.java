package org.example.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;

public class ActionBench extends CrailAction {
  public static final String FILE_SUFFIX = "-data";
  private CrailFile crailFile;
  private long bytesToWrite = 1024 * 1024 * 1024;

  @Override
  public void onCreate() {
    try {
      crailFile = this.fs.create(this.self.getPath() + FILE_SUFFIX, CrailNodeType.DATAFILE,
          CrailStorageClass.get(1), CrailLocationClass.DEFAULT, false)
          .get().asFile();
    } catch (Exception e) {
      System.out.println("Error creating data file for action " + self.getPath());
      e.printStackTrace();
    }
  }

  @Override
  public void onRead(WritableByteChannel channel) {
    long time1 = System.currentTimeMillis();
    long read = BenchCrail.readFileFromCrail(crailFile);
    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) read / 1024;

    System.out.println("READ");
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    try {
      ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES);
      buff.putInt(0).flip();
      channel.write(buff);
      channel.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void onWrite(ReadableByteChannel channel) {
    long time1 = System.currentTimeMillis();
    BenchCrail.writeBytesToCrail(bytesToWrite, crailFile);
    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) bytesToWrite / 1024;

    System.out.println("WRITE");
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    try {
      channel.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onDelete() {
    try {
      this.fs.delete(crailFile.getPath(), true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
