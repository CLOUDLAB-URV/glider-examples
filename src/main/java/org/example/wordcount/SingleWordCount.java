package org.example.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveWritableChannel;

/**
 * Local WordCount computation that first filters the lines. It operates on a
 * file stored (or first loaded) in Crail as a simple CrailFile.
 * This implementation works on a single file and the computation is
 * single-threaded.
 * <p>
 * Contains 3 implementations:
 * {@link SingleWordCount#runLocal()},
 * {@link SingleWordCount#runCrailFile()}, and
 * {@link SingleWordCount#runCrailActive()}.
 */
public class SingleWordCount {
  public static final String FILENAME = "/Datasets/wiki100/AA/wiki_00";
  public static final String crailPath = "/wiki_00";

  private static CrailConfiguration conf;
  private static CrailStore store;

  static {
    try {
      conf = CrailConfiguration.createConfigurationFromFile();
      store = CrailStore.newInstance(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    try {
//      runLocal();
//      runCrailFile();
      runCrailActive();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Filtering is performed on Crail. The file is stored on Crail as a
   * {@link FilterAction}, which returns only the filtered data when read.
   *
   * @throws Exception
   */
  public static void runCrailActive() throws Exception {
    Path path = Paths.get(FILENAME);

    CrailNode crailNode = store.lookup(crailPath).get();
    CrailObjectProxy filterAction;
    if (crailNode == null) {
      crailNode = store.create(crailPath, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
                               CrailLocationClass.DEFAULT, false).get();
      filterAction = crailNode.asObject().getProxy();
      filterAction.create(FilterAction.class);

      try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
        ActiveWritableChannel actionChannel = filterAction.getWritableChannel();
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024); // 16 KB buffer

        while (channel.read(buffer) != -1) {
          buffer.flip();
          actionChannel.write(buffer);
          buffer.clear();
        }
        actionChannel.close();
      }
    } else {
      filterAction = crailNode.asObject().getProxy();
    }

    InputStream is = filterAction.getInputStream();
    Stream<String> lines = new BufferedReader(new InputStreamReader(is)).lines();

    long time1 = System.currentTimeMillis();
    Map<String, Long> words = countWordsLocal(lines);
    long time2 = System.currentTimeMillis();
    List<Map.Entry<String, Long>> top10 = getTop10(words);
    System.out.println(top10);

    System.out.println("Elapsed: " + (time2 - time1) + " ms");
  }

  /**
   * All computation is local. The file is stored/loaded as a
   * <code>CrailFile</code>.
   *
   * @throws IOException
   */
  public static void runCrailFile() throws Exception {
    CrailNode crailNode = store.lookup(crailPath).get();
    if (crailNode == null) {
      System.out.println("Loading file to Crail... " + FILENAME);
      crailNode = store.create(crailPath, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
                               CrailLocationClass.DEFAULT, false).get();
      sendFileToCrail(FILENAME, crailNode.asFile());
      System.out.println("Done");
    }

    CrailFile crailFile = crailNode.asFile();

    CrailBufferedInputStream crailBufferedInputStream = crailFile.getBufferedInputStream(crailNode.getCapacity());
    Stream<String> lines = new BufferedReader(new InputStreamReader(crailBufferedInputStream)).lines();
    long time1 = System.currentTimeMillis();
    Map<String, Long> words = countWordsLocal(lines);
    long time2 = System.currentTimeMillis();
    List<Map.Entry<String, Long>> top10 = getTop10(words);
    System.out.println(top10);

    System.out.println("Elapsed: " + (time2 - time1) + " ms");

    crailBufferedInputStream.close();
    store.close();
  }

  /**
   * Full local implementation. The file is loaded from local filesystem.
   *
   * @throws IOException
   */
  public static void runLocal() throws IOException {
    Stream<String> lines = Files.lines(Paths.get(FILENAME));
    long time1 = System.currentTimeMillis();
    Map<String, Long> words = countWordsLocal(lines);
    long time2 = System.currentTimeMillis();
    List<Map.Entry<String, Long>> top10 = getTop10(words);
    System.out.println(top10);

    System.out.println("Elapsed: " + (time2 - time1) + " ms");
  }

  public static Map<String, Long> countWordsLocal(Stream<String> lines) {
    return lines.parallel()
                .flatMap(line -> Stream.of(line.toLowerCase().split("\\W+"))
                                       .filter(w -> !w.isEmpty()))
                .collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting()));
  }

  private static List<Map.Entry<String, Long>> getTop10(Map<String, Long> words) {
    return words.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(10)
                .collect(Collectors.toList());
  }

  /**
   * Loads a local file at <code>filename</code> up to Crail servers as a simple
   * <code>CrailFile</code>. Lazy read-write with 8 KB buffer.
   *
   * @param filename  Path to local file.
   * @param crailFile Crail file descriptor object.
   */
  private static void sendFileToCrail(String filename, CrailFile crailFile) {
    // could be improved with memory-mapped file?
    Path path = Paths.get(filename);
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      CrailBufferedOutputStream crailBufferedOutputStream =
          crailFile.getBufferedOutputStream(channel.size());

      ByteBuffer buffer = ByteBuffer.allocate(8 * 1024); // 8 KB buffer

      while (channel.read(buffer) != -1) {
        buffer.flip();
        crailBufferedOutputStream.write(buffer);
        buffer.rewind();
      }
      crailBufferedOutputStream.close();
    } catch (IOException e) {
      System.out.println("Cannot open/send file: " + path);
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
    }
  }
}
