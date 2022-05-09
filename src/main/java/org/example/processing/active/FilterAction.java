package org.example.processing.active;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.stream.Stream;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailFile;

public class FilterAction extends CrailAction {
  String filename;
  CrailFile originalData;

  @Override
  public void onCreate() {
    filename = this.self.getPath().replace("filter", "wiki");
    try {
      originalData = this.fs.lookup(filename).get().asFile();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onRead(WritableByteChannel channel) {
    try {
      CrailBufferedInputStream cbis = originalData.getBufferedInputStream(originalData.getCapacity());

      Stream<String> lines = new BufferedReader(new InputStreamReader(cbis)).lines();
      Stream<String> filteredLines = lines.filter(l -> l.contains("cosmos"));

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel)));
      filteredLines.forEach(line -> {
        try {
          writer.write(line);
          writer.newLine();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      writer.close();

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
