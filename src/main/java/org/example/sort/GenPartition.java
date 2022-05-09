package org.example.sort;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;

public class GenPartition implements Runnable {
  private CrailStore store;
  private String filename;
  private Random random;
  private int rows;
  private int columns;

  public GenPartition(CrailStore store, String filename, int rows, int columns) {
    this.store = store;
    this.filename = filename;
    this.rows = rows;
    this.columns = columns;
    this.random = new Random();
  }

  public GenPartition(CrailStore store, String filename, int rows, int columns, long seed) {
    this.store = store;
    this.filename = filename;
    this.rows = rows;
    this.columns = columns;
    this.random = new Random(seed);
  }

  @Override
  public void run() {
    try {
      CrailFile crailFile = store.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
          CrailLocationClass.DEFAULT, false).get().asFile();

      CrailBufferedOutputStream cbos = crailFile.getBufferedOutputStream(0);
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(cbos));
      for (int r = 0; r < rows; r++) {
        String row = random.ints(columns).mapToObj(String::valueOf).collect(Collectors.joining(","));
        writer.write(row);
        writer.newLine();
      }
      writer.close();

      System.out.println("Generated file " + filename + " of size " + crailFile.getCapacity());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
