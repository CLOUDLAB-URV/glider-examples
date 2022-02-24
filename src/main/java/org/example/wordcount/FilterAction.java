package org.example.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
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
  public void onReadStream(WritableByteChannel channel) {
    System.out.println("Filter action on read stream: " + this.self.getPath());
    // Just return what is on the crail file
    try {
      ByteBuffer buffer = ByteBuffer.allocate(8 * 1024);
      CrailBufferedInputStream bufferedStream =
          myData.getBufferedInputStream(8 * 1024);
      while (bufferedStream.read(buffer) != -1) {
        buffer.flip();
        channel.write(buffer);
        buffer.clear();
      }
      bufferedStream.close();
      channel.close();
    } catch (Exception e) {
      System.out.println("Error reading from data file for action " + self.getPath());
      e.printStackTrace();
    }
  }

  @Override
  public void onWriteStream(ReadableByteChannel channel) {
    // Process received data: filter lines, store to crail file only filtered data
    System.out.println("Filter action on write stream: " + this.self.getPath());

    InputStream stream = Channels.newInputStream(channel);
    Stream<String> lines = new BufferedReader(new InputStreamReader(stream)).lines();
    Stream<String> linesWithGold = lines.filter(l -> l.contains("gold"));

    try {
      CrailBufferedOutputStream outputStream = myData.getBufferedOutputStream(0);
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
      linesWithGold.forEachOrdered(l -> {
        try {
          writer.write(l);
          writer.newLine();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      writer.close();
    } catch (Exception e) {
      System.out.println("Error writing to crail data file for action " + self.getPath());
      e.printStackTrace();
    }
  }

  @Override
  public void onDelete() {
    super.onDelete();
  }
}
