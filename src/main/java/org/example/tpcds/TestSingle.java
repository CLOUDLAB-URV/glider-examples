package org.example.tpcds;

import java.util.Map;
import java.util.concurrent.Callable;

public class TestSingle {
  private static final String BASE_DIR = "/home/daniel/data/tpcds-s1/";

  /** Runs query15 in a single task.
   * 
   * @param args Program args.
   */
  public static void main(String[] args) {
    try {
      Callable<Map<String, Double>> task = new QueryWorker(0, 0, 0, 0, BASE_DIR);
      
      long start = System.currentTimeMillis();

      Map<String, Double> top100 = task.call();

      long end = System.currentTimeMillis();

      System.out.println("Run in " + (end - start) / 1000.0 + " s");
      top100.forEach((k, v) -> System.out.printf("%s -> %.2f%n", k, v));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}