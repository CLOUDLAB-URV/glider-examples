package org.example.sort.active;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Stream;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;

public class ReduceAction extends CrailAction {
  private CrailFile resultFile;
  private int columnKey;

  private List<SimpleEntry<Integer, String>> allLines;


  @Override
  public void onCreate() {
    columnKey = 0; // TODO: pass it
    try {
      resultFile = this.fs.create(this.self.getPath().replace("redu", "result"),
          CrailNodeType.DATAFILE, CrailStorageClass.get(1),
          CrailLocationClass.DEFAULT, true).get().asFile();
    } catch (Exception e) {
      e.printStackTrace();
    }
    allLines = new ArrayList<>();
  }

  @Override
  public void onRead(WritableByteChannel channel) {
    try {
      CrailBufferedOutputStream cbos = resultFile.getBufferedOutputStream(1024 * 1024 * 500);
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(cbos));
      // for (List<String> list : sort.values()) {
      // for (String line : list) {
      // writer.write(line);
      // writer.newLine();
      // }
      allLines.sort(Comparator.comparingInt(SimpleEntry::getKey));
      allLines.forEach(entry -> {
        try {
          writer.write(entry.getValue());
          writer.newLine();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      // allLines.stream()
      // .sorted(Comparator.comparingInt(SimpleEntry::getKey))
      // .forEachOrdered(entry -> {
      // try {
      // writer.write(entry.getValue());
      // writer.newLine();
      // } catch (IOException e) {
      // e.printStackTrace();
      // }
      // });
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      channel.write(ByteBuffer.allocate(Integer.BYTES));
      channel.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onWrite(ReadableByteChannel channel) {
    Stream<String> lines = new BufferedReader(new InputStreamReader(Channels.newInputStream(channel))).lines();
    lines
        .map(line -> new SimpleEntry<>(Integer.parseInt(line.split(",", columnKey + 2)[columnKey]), line))
        .forEach(entry -> allLines.add(entry));
  }

}
