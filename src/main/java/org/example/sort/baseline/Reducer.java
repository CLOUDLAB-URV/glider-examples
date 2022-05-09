package org.example.sort.baseline;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Comparator;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Stream;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailMultiFile;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;

public class Reducer implements Runnable {
  private CrailMultiFile multiFile;
  private CrailFile resultFile;
  private int workers;
  private int columnKey;

  public Reducer(CrailStore store, int columnKey,
      String reducerMultifile, String resultFileName, int workers) throws Exception {
    this.workers = workers;
    this.columnKey = columnKey;

    multiFile = store.lookup(reducerMultifile).get().asMultiFile();
    resultFile = store.create(resultFileName, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, true)
        .get().asFile();
  }

  @Override
  public void run() {
    try {
      CrailBufferedInputStream multiStream = multiFile.getMultiStream(workers);
      CrailBufferedOutputStream cbos = resultFile.getBufferedOutputStream(multiFile.getCapacity());
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(cbos));

      Stream<String> lines = new BufferedReader(new InputStreamReader(multiStream)).lines();
      lines
          .map(line -> new SimpleEntry<>(Integer.parseInt(line.split(",", columnKey + 2)[columnKey]), line))
          .sorted(Comparator.comparingInt(SimpleEntry::getKey))
          .forEachOrdered(entry -> {
            try {
              writer.write(entry.getValue());
              writer.newLine();
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
      // Equivalent to:
      // List<SimpleEntry<Integer, String>> allLines = lines
      //     .map(line -> new SimpleEntry<>(Integer.parseInt(line.split(",", columnKey + 2)[columnKey]), line))
      //     .collect(Collectors.toList());
      // allLines.sort(Comparator.comparingInt(SimpleEntry::getKey));
      // allLines.forEach(entry -> {
      //   try {
      //     writer.write(entry.getValue());
      //     writer.newLine();
      //   } catch (IOException e) {
      //     e.printStackTrace();
      //   }
      // });
      
      writer.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
