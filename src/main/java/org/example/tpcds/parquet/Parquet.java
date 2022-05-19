package org.example.tpcds.parquet;

import java.util.List;
import java.util.Map;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;

public class Parquet {
  private Map<Integer, Group> data;
  private List<Type> schema;

  public Parquet(Map<Integer, Group> data, List<Type> schema) {
    this.data = data;
    this.schema = schema;
  }

  public Map<Integer, Group> getData() {
    return data;
  }

  public List<Type> getSchema() {
    return schema;
  }
}
