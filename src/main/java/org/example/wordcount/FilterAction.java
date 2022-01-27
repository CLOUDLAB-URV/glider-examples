package org.example.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;

/**
 * Action that saves data written to it into another (traditional) Crail File.
 * The received data is however first filtered (string-based).
 */
public class FilterAction extends CrailAction {
  private static final String FILE_SUFFIX = "-data";
  private CrailFile myData;

  @Override
  public void onCreate() {
    System.out.println("Filter action on create: " + this.self.getPath());

    try {
      // Create the delegated crail file
      myData = this.fs.create(this.self.getPath() + FILE_SUFFIX, CrailNodeType.DATAFILE,
              CrailStorageClass.get(1), CrailLocationClass.DEFAULT, false)
          .get().asFile();
    } catch (Exception e) {
      System.out.println("Error creating data file for action " + self.getPath());
      e.printStackTrace();
    }
  }

  @Override
  public void onRead(ByteBuffer buffer) {
    System.out.println("Filter action on read: " + this.self.getPath());
    // Just return what is on the crail file
    try {
      CrailBufferedInputStream bufferedStream =
          myData.getBufferedInputStream(buffer.remaining());
      bufferedStream.read(buffer);
      bufferedStream.close();
    } catch (Exception e) {
      System.out.println("Error reading from data file for action " + self.getPath());
      e.printStackTrace();
    }
  }

  @Override
  public int onWrite(ByteBuffer buffer) {
    // Process received data: filter lines, store to crail file only filtered data
    System.out.println("Filter action on write: " + this.self.getPath());

    byte[] input = new byte[buffer.remaining()];
    buffer.get(input);
    ByteArrayInputStream is = new ByteArrayInputStream(input);
    Stream<String> lines = new BufferedReader(new InputStreamReader(is)).lines();
    Stream<String> linesWithGold = lines.filter(l -> l.contains("gold"));

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os));
    linesWithGold.forEachOrdered(l -> {
      try {
        writer.write(l);
        writer.newLine();
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    try {
      byte[] output = os.toByteArray();
      CrailBufferedOutputStream outputStream =
          myData.getBufferedOutputStream(output.length);
      outputStream.write(output);
      outputStream.close();
      return output.length;
    } catch (Exception e) {
      System.out.println("Error writing to data file for action " + self.getPath());
      e.printStackTrace();
      return -1;
    }
  }

  @Override
  public void onDelete() {
    super.onDelete();
  }
}
