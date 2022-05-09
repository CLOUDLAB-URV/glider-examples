package org.example.sort.baseline;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.example.sort.GenPartition;

public class Client {
  private int workers;
  private int columnKey;
  private int rows;
  private int columns;
  private String baseCrailpath;
  private String reduDirName;
  private String resultFileName;
  private ExecutorService es;
  private CrailConfiguration conf;
  private CrailStore store;

  public Client(int workers, String basePath, int rows, int columns, int columnKey) throws Exception {
    this.conf = CrailConfiguration.createConfigurationFromFile();
    this.workers = workers;
    this.baseCrailpath = basePath;
    this.rows = rows;
    this.columns = columns;
    this.columnKey = columnKey;
    this.es = Executors.newFixedThreadPool(16);
    this.reduDirName = baseCrailpath + "-redu";
    this.resultFileName = baseCrailpath + "-result";

    this.store = CrailStore.newInstance(conf);
  }

  public void genData() throws Exception {
    List<Future<?>> futures = new ArrayList<>(workers);

    String partName;
    for (int i = 0; i < workers; i++) {
      partName = baseCrailpath + "_part" + i;
      futures.add(es.submit(new GenPartition(store, partName, rows, columns)));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  public void createReduFiles() throws Exception {
    store.create(reduDirName, CrailNodeType.DIRECTORY, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, false).get();
    String multifileName;
    for (int i = 0; i < workers; i++) {
      multifileName = reduDirName + "/group_" + i;
      store.create(multifileName, CrailNodeType.MULTIFILE, CrailStorageClass.get(1),
          CrailLocationClass.DEFAULT, false).get();
    }
  }

  public void createResultFile() throws Exception {
    store.create(resultFileName, CrailNodeType.MULTIFILE, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, false).get();
  }

  public void deleteReduFiles() throws Exception {
    String multifileName;
    for (int i = 0; i < workers; i++) {
      multifileName = reduDirName + "/group_" + i;
      try {
        store.delete(multifileName, true).get();
      } catch (Exception e) {
        System.out.println(e);
        // e.printStackTrace();
      }
    }
    try {
      store.delete(reduDirName, true).get();
    } catch (Exception e) {
      System.out.println(e);
      // e.printStackTrace();
    }
  }

  public void deleteResultFile() throws Exception {
    try {
      store.delete(resultFileName, true).get();
    } catch (Exception e) {
      System.out.println(e);
      // e.printStackTrace();
    }
  }

  public void runMap() throws Exception {
    List<Future<?>> futures = new ArrayList<>(workers);
    List<CrailStore> stores = new ArrayList<>(workers);

    String partName, groupsPattern;
    for (int i = 0; i < workers; i++) {
      CrailStore s = CrailStore.newInstance(conf);
      stores.add(s);

      partName = baseCrailpath + "_part" + i;
      groupsPattern = reduDirName + "/group_%d/part_" + i;
      futures.add(es.submit(new Mapper(s, partName, columnKey, groupsPattern, workers)));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    stores.forEach(s -> {
      try {
        s.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  public void runReduce() throws Exception {
    List<Future<?>> futures = new ArrayList<>(workers);
    List<CrailStore> stores = new ArrayList<>(workers);

    String reduMultifile;
    for (int i = 0; i < workers; i++) {
      CrailStore s = CrailStore.newInstance(conf);
      stores.add(s);

      reduMultifile = reduDirName + "/group_" + i;
      futures.add(es.submit(new Reducer(s, columnKey, reduMultifile, resultFileName + "/r" + i, workers)));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    stores.forEach(s -> {
      try {
        s.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  public void deleteData() throws Exception {
    String partName;
    for (int i = 0; i < workers; i++) {
      try {
        partName = baseCrailpath + "_part" + i;
        store.delete(partName, true).get();
      } catch (Exception e) {
        System.out.println(e);
        // e.printStackTrace();
      }
    }
  }

  public void close() {
    es.shutdown();
    try {
      store.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      if (!es.awaitTermination(30, TimeUnit.SECONDS)) {
        System.out.println("Executor did not terminate");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void checkResult() throws Exception {
    System.out.println("\n\nChecking result is sorted...");
    int previousKey = Integer.MIN_VALUE;
    long nlines = 0;
    for (int i = 0; i < workers; i++) {
      CrailFile file = store.lookup(resultFileName + "/r" + i).get().asFile();
      System.out.println("File " + file.getPath() + " is " + file.getCapacity());
      CrailBufferedInputStream is = file.getBufferedInputStream(file.getCapacity());
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
        String line = reader.readLine();
        while (line != null) {
          int currentKey = Integer.parseInt(line.split(",", columnKey + 2)[columnKey]);
          if (currentKey < previousKey) {
            System.out.println("Not sorted!!");
            return;
          }
          previousKey = currentKey;
          line = reader.readLine();
          nlines++;
        }
      }
    }
    System.out.println("Sort OK!!");
    long expectedLines = workers * (long) rows;
    System.out.print("Expected lines " + expectedLines + " got " + nlines);
    if (expectedLines == nlines) {
      System.out.println(" OK");
    } else {
      System.out.println(" KO");
    }
  }

  public static void main(String[] args) {
    // Defaults:
    int workers = 1;
    String filename = "/sort-data";
    int rows = 5000000;
    int columns = 10;
    int columnKey = 0;
    boolean generate = false;
    boolean delete = false;
    boolean exclusive = false;
    boolean noCheck = false;

    Option helpOption = Option.builder("h").desc("show this help message").build();
    Option workersOption = Option.builder("w").desc("number of workers [1..n]").hasArg().build();
    Option columnKeyOption = Option.builder("k").desc("column index to use as sort key [0..c]").hasArg().build();
    Option fileOption = Option.builder("f").desc("base path for crail files").hasArg().build();
    Option generateOption = Option.builder("g").desc("generate partition files").build();
    Option rowsOption = Option.builder("r").desc("for generate: number of rows per partition [1..n]").hasArg().build();
    Option columnsOption = Option.builder("c").desc("for generate: number of columns per row [1..n]").hasArg().build();
    Option deleteOption = Option.builder("d").desc("delete the generated partition files").build();
    Option exclusiveOption = Option.builder("x").desc("skip mapreduce (to only generate or eliminate data)").build();
    Option noCheckOption = Option.builder("z").desc("skip checking result").longOpt("noCheck").build();

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(workersOption);
    options.addOption(columnKeyOption);
    options.addOption(fileOption);
    options.addOption(generateOption);
    options.addOption(rowsOption);
    options.addOption(columnsOption);
    options.addOption(deleteOption);
    options.addOption(exclusiveOption);
    options.addOption(noCheckOption);

    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine line = parser.parse(options, args);

      if (line.hasOption(helpOption.getOpt())) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("sort", options);
        System.exit(-1);
      }

      if (line.hasOption(workersOption.getOpt())) {
        workers = Integer.parseInt(line.getOptionValue(workersOption.getOpt()));
      }
      if (line.hasOption(columnKeyOption.getOpt())) {
        columnKey = Integer.parseInt(line.getOptionValue(columnKeyOption.getOpt()));
      }
      if (line.hasOption(fileOption.getOpt())) {
        filename = line.getOptionValue(fileOption.getOpt());
      }
      generate = line.hasOption(generateOption.getOpt());
      if (line.hasOption(rowsOption.getOpt())) {
        rows = Integer.parseInt(line.getOptionValue(rowsOption.getOpt()));
      }
      if (line.hasOption(columnsOption.getOpt())) {
        columns = Integer.parseInt(line.getOptionValue(columnsOption.getOpt()));
      }
      delete = line.hasOption(deleteOption.getOpt());
      exclusive = line.hasOption(exclusiveOption.getOpt());
      noCheck = line.hasOption(noCheckOption.getOpt());

    } catch (ParseException e) {
      System.err.println("Could not parse options.");
      e.printStackTrace();
      System.exit(-1);
    }

    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    try {
      Client client = new Client(workers, filename, rows, columns, columnKey);

      long stt = System.currentTimeMillis();
      if (generate) {
        client.genData();
      }
      long sot = System.currentTimeMillis();
      // SORT
      if (!exclusive) {
        client.createReduFiles();
        client.runMap();
      }
      long mpt = System.currentTimeMillis();
      System.out.println("\nMAP DONE\n");
      if (!exclusive) {
        client.createResultFile();
        client.runReduce();
      }
      long edt = System.currentTimeMillis();
      if (!exclusive) {
        client.deleteReduFiles();
        if (!noCheck)
          client.checkResult();
        client.deleteResultFile();
      }

      if (delete) {
        client.deleteData();
      }

      client.close();

      StringBuilder setup = new StringBuilder("\nSort setup:\n");
      setup.append(String.format("Workers: %d\n", workers));
      setup.append(String.format("Base path: %s\n", filename));
      setup.append(String.format("Sorting key: %s\n", columnKey));
      if (generate) {
        setup.append(String.format("Generate %d partitions of %d rows and %d columns.\n", workers, rows, columns));
      }
      if (delete) {
        setup.append("Delete generated data.\n");
      }
      if (exclusive) {
        setup.append("Skip mapreduce execution.\n");
      }
      if (noCheck) {
        setup.append("Skip checking result.\n");
      }
      System.out.println(setup);

      long totalElapsed = edt - stt;
      long genElapsed = sot - stt;
      long mapElapsed = mpt - sot;
      long reduceElapsed = edt - mpt;
      StringBuilder report = new StringBuilder("Time report (s):\n");
      report.append(String.format("Total: %.3f\n", (double) totalElapsed / 1000));
      report.append(String.format("Generate data: %.3f\n", (double) genElapsed / 1000));
      report.append(String.format("Sort: %.3f\n", (double) (edt - sot) / 1000));
      report.append(String.format(" | Map: %.3f\n", (double) mapElapsed / 1000));
      report.append(String.format(" | Reduce: %.3f\n", (double) reduceElapsed / 1000));

      System.out.println(report);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
