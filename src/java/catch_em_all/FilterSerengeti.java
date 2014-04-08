package catch_em_all;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.*;

public class FilterSerengeti extends BaseFilter {
  public boolean isKeep(TridentTuple t) {
    Map<String,Object> classification = (Map<String,Object>) t.getValueByField("classification");
    String project = (String) classification.get("project");
    return project.equals("serengeti");
  }
}
