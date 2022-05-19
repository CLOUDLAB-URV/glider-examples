package org.example.tpcds.parquet;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class Reader {

  private Reader() {
  }

  /**
   * Read a parquet file fully.
   * 
   * @param filePath Path to parquet file
   * @param keyField Key Field name
   * @return An boject with the file records.
   * @throws IOException Error reading file.
   */
  public static Parquet getParquetData(String filePath, String keyField)
      throws IOException {
    Map<Integer, Group> simpleGroups = new HashMap<>();
    List<Type> fields;
    try (ParquetFileReader reader = ParquetFileReader
        .open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      fields = schema.getFields();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        for (int i = 0; i < rows; i++) {
          Group simpleGroup = recordReader.read();
          try {
            simpleGroups.put(simpleGroup.getInteger(keyField, 0), simpleGroup);
          } catch (Exception e) {
            // ignore
          }
        }
      }
    }
    return new Parquet(simpleGroups, fields);
  }

  /**
   * Read a parquet file but only the given fields.
   * 
   * @param filePath Path to parquet file
   * @param keyField Key Field name
   * @param namesArr Array of field names to be read.
   * @return An boject with the file records.
   * @throws IOException Error reading file.
   */
  public static Parquet getParquetData(String filePath, String keyField, String... namesArr)
      throws IOException {
    Map<Integer, Group> simpleGroups = new HashMap<>();
    List<Type> fields;
    try (ParquetFileReader reader = ParquetFileReader
        .open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      List<String> names = Arrays.asList(namesArr);
      fields = schema.getFields().stream()
          .filter(field -> names.contains(field.getName()))
          .collect(Collectors.toList());
      schema = new MessageType(schema.getName(), fields);

      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        for (int i = 0; i < rows; i++) {
          Group simpleGroup = recordReader.read();
          try {
            simpleGroups.put(simpleGroup.getInteger(keyField, 0), simpleGroup);
          } catch (Exception e) {
            // ignore
          }
        }
      }
    }
    return new Parquet(simpleGroups, fields);
  }

  /**Runs some logic per each row read.
   * 
   * @param filePath Parquet file to read.
   * @param consumer Logic that consumes each row.
   * @param arrNames Fields to read.
   * @throws IOException Error reading file.
   */
  public static void runPerRow(String filePath, Consumer<Group> consumer, String... arrNames)
      throws IOException {
    try (ParquetFileReader parquetReader = ParquetFileReader
        .open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()))) {
      MessageType schema = parquetReader.getFooter().getFileMetaData().getSchema();
      List<String> names = Arrays.asList(arrNames);
      List<Type> fields = schema.getFields().stream()
          .filter(field -> names.contains(field.getName())).collect(Collectors.toList());
      schema = new MessageType(schema.getName(), fields);
      PageReadStore pages;
      while ((pages = parquetReader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        for (int i = 0; i < rows; i++) {
          Group row = recordReader.read();
          consumer.accept(row);
        }
      }
    }
  }
}
