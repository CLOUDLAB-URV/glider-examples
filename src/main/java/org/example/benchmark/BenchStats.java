package org.example.benchmark;

import java.util.LinkedList;
import java.util.List;

public class BenchStats {
  // Mbps
  List<Double> bandwidthRead;
  List<Double> bandwidthWrite;
  // s
  List<Double> latencyRead;
  List<Double> latencyWrite;

  public BenchStats() {
    bandwidthRead = new LinkedList<>();
    bandwidthWrite = new LinkedList<>();
    latencyRead = new LinkedList<>();
    latencyWrite = new LinkedList<>();
  }

  public void addBendwidthRead(double newBw) {
    bandwidthRead.add(newBw);
  }

  public void addBendwidthWrite(double newBw) {
    bandwidthWrite.add(newBw);
  }

  public void addLatencyRead(double newLat) {
    latencyRead.add(newLat);
  }

  public void addLatencyWrite(double newLat) {
    latencyWrite.add(newLat);
  }

  public double getBwRead() {
    return bandwidthRead.stream().mapToDouble(i -> i).average().orElseGet(() -> 0);
  }

  public double getBwWrite() {
    return bandwidthWrite.stream().mapToDouble(i -> i).average().orElseGet(() -> 0);
  }

  public double getLatRead() {
    return latencyRead.stream().mapToDouble(i -> i).average().orElseGet(() -> 0);
  }

  public double getLatWrtie() {
    return latencyWrite.stream().mapToDouble(i -> i).average().orElseGet(() -> 0);
  }

}
