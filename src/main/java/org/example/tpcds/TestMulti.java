package org.example.tpcds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestMulti {
  private static final String BASE_DIR = "/home/daniel/data/tpcds-s5-p4/";
  private static final int N_PARTS = 1;

  private final ExecutorService es;
  private int parallelism = 1;

  public TestMulti() {
    es = Executors.newFixedThreadPool(parallelism);
  }

  public void close() {
    es.shutdown();
    try {
      if (!es.awaitTermination(30, TimeUnit.SECONDS)) {
        System.out.println("Executor did not terminate in 30.");
        es.shutdownNow();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Run the query in parallel.
   * 
   * @return The result of the query
   * @throws ExecutionException   A task did not complete successfully.
   * @throws InterruptedException Thread interrupted while running the tasks.
   */
  public Map<String, Double> run() throws ExecutionException, InterruptedException {
    List<Callable<Map<String, Double>>> tasks = new ArrayList<>();

    for (int i = 0; i < N_PARTS; i++) {
      try {
        tasks.add(new QueryWorker(i, BASE_DIR));
      } catch (IOException e) {
        System.err.println("Could not create task for partition " + i);
        e.printStackTrace();
      }
    }
    List<Future<Map<String, Double>>> futures = es.invokeAll(tasks);
    List<Map<String, Double>> results = new ArrayList<>(futures.size());
    for (Future<Map<String, Double>> future : futures) {
      results.add(future.get());
    }

    Map<String, Double> totals = results.stream()
        .flatMap(m -> m.entrySet().stream())
        .collect(Collectors.groupingBy(Map.Entry::getKey,
            Collectors.summingDouble(Map.Entry::getValue)));

    return totals.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .limit(100)
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (e1, e2) -> e1,
            LinkedHashMap::new));
  }

  /**
   * Runs query15 in multiple parallel tasks.
   * Each task processes a combination of dataset partitions.
   * 
   * @param args Program args.
   */
  public static void main(String[] args) {

    TestMulti test = new TestMulti();

    try {
      long start = System.currentTimeMillis();
      Map<String, Double> result = test.run();
      long end = System.currentTimeMillis();

      System.out.println("Run in " + (end - start) / 1000.0 + " s");
      result.forEach((k, v) -> System.out.printf("%s -> %.2f%n", k, v));
    } catch (ExecutionException e) {
      // A task did not complete successfully.
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    } finally {
      test.close();
    }
  }
}
