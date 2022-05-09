package org.example.sort.baseline;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;

public class Mapper implements Runnable {

  // private CrailStore store;
  private int workers;
  private int columnKey;
  private long[] groupBuckets;
  private CrailFile partFile;
  private CrailFile[] groupFiles;

  public Mapper(CrailStore store, String partitionFile, int columnKey,
      String groupsFilePattern, int workers) throws Exception {
    // this.store = store;
    this.workers = workers;
    this.columnKey = columnKey;

    groupBuckets = new long[workers];
    long totalRange = (long) Integer.MAX_VALUE - Integer.MIN_VALUE;
    long groupLength = totalRange / workers;
    long currLimit = Integer.MIN_VALUE;
    for (int i = 0; i < groupBuckets.length; i++) {
      groupBuckets[i] = currLimit;
      currLimit += groupLength;
    }
    // System.out.println("Range: " + Integer.MIN_VALUE + " - " +
    // Integer.MAX_VALUE);
    // System.out.println(Arrays.toString(groupBuckets));

    partFile = store.lookup(partitionFile).get().asFile();
    groupFiles = new CrailFile[workers];
    for (int i = 0; i < groupFiles.length; i++) {
      String filename = String.format(groupsFilePattern, i);
      groupFiles[i] = store.create(filename, CrailNodeType.DATAFILE,
          CrailStorageClass.get(1), CrailLocationClass.DEFAULT, true).get().asFile();
    }
  }

  @Override
  public void run() {
    try {
      CrailBufferedInputStream cbis = partFile.getBufferedInputStream(partFile.getCapacity());
      BufferedReader reader = new BufferedReader(new InputStreamReader(cbis));

      BufferedWriter[] writers = new BufferedWriter[workers];
      for (int i = 0; i < groupFiles.length; i++) {
        CrailBufferedOutputStream cbos = groupFiles[i].getBufferedOutputStream(partFile.getCapacity()/workers);
        writers[i] = new BufferedWriter(new OutputStreamWriter(cbos));
      }

      reader.lines().forEach(line -> {
        int sortKey = Integer.parseInt(line.split(",", columnKey + 2)[columnKey]);
        int group;
        for (group = workers - 1; group >= 0; group--) {
          if (sortKey >= groupBuckets[group]) {
            break;
          }
        }
        // System.out.println("Key " + sortKey + " sorted to group " + group);
        try {
          writers[group].write(line);
          writers[group].newLine();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });

      for (int i = 0; i < writers.length; i++) {
        writers[i].close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
