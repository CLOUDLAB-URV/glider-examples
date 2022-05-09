package org.example.reduction.baseline;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailMultiFile;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;

public class Client {
  private static int N_KEYS = 1024;
  private static int N_WORKERS = 10;
  private static long WORKER_OPS = 50000000;

  private static String BAG_PATH = "/bag";
  private static String RESULT_PATH = "/result";

  public static void main(String[] args) throws Exception {
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    // Create bag
    CrailMultiFile bag = store.create(BAG_PATH, CrailNodeType.MULTIFILE,
        CrailStorageClass.get(1), CrailLocationClass.DEFAULT, false)
        .get().asMultiFile();

    long start = System.currentTimeMillis();

    // Run workers
    ExecutorService es = Executors.newFixedThreadPool(N_WORKERS);
    List<Future<?>> futures = new ArrayList<>(N_WORKERS);

    for (int i = 0; i < N_WORKERS; i++) {
      futures.add(es.submit(new Worker(i)));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    long mid = System.currentTimeMillis();

    // Read bag and reduce
    CrailBufferedInputStream multiStream = bag.getMultiStream(N_WORKERS);
    Stream<String> lines = new BufferedReader(new InputStreamReader(multiStream)).lines();
    Map<Integer, Long> result = lines.collect(
        Collectors.toMap(line -> Integer.parseInt(line.split(",")[0]),
            line -> Long.parseLong(line.split(",")[1]),
            (val, acc) -> val + acc));

    // Store result to crail
    CrailFile resultFile = store.create(RESULT_PATH, CrailNodeType.DATAFILE,
        CrailStorageClass.get(1), CrailLocationClass.DEFAULT, false)
        .get().asFile();
    CrailBufferedOutputStream cbos = resultFile.getBufferedOutputStream(0);
    ObjectOutputStream oos = new ObjectOutputStream(cbos);
    oos.writeObject(result);
    oos.close();

    long end = System.currentTimeMillis();

    // Elapsed time includes from launching workers until getting the reduced result
    // (and put to crail)
    double elapsedSecs = (double) (end - start) / 1000;
    double elapsedWorkers = (double) (mid - start) / 1000;
    System.out.println("Elapsed: " + elapsedSecs + " s");
    System.out.println("Elapsed workers: " + elapsedWorkers + " s");
    System.out.println("Result size: " + resultFile.getCapacity());

    // Clean up
    es.shutdown();

    try {
      store.delete(BAG_PATH, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      store.delete(RESULT_PATH, true).get();
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

    try {
      store.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public static class Worker implements Runnable {
    private int workerId;

    public Worker(int id) {
      this.workerId = id;
    }

    @Override
    public void run() {
      try {
        // Each worker has a different store to simulate they are independent and avoid
        // network lock
        CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
        CrailStore store = CrailStore.newInstance(conf);

        String filename = BAG_PATH + "/" + workerId;
        CrailFile crailFile = store.create(filename, CrailNodeType.DATAFILE,
            CrailStorageClass.get(1), CrailLocationClass.DEFAULT, true)
            .get().asFile();

        Random random = new Random();
        CrailBufferedOutputStream cbos = crailFile.getBufferedOutputStream(WORKER_OPS);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(cbos));
        for (int i = 0; i < WORKER_OPS; i++) {
          try {
            writer.write(String.join(",", String.valueOf(random.nextInt(N_KEYS)), String.valueOf(random.nextLong())));
            writer.newLine();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        writer.close();

        System.out.println("Total file " + workerId + ": " + crailFile.getCapacity());

        store.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
