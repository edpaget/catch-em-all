package catch_em_all;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.*;

public class SplitAnimals extends BaseFunction {
  public void execute(TridentTuple tuple, TridentCollector collector) {
    Map<String,Object> classification = (Map<String,Object>) tuple.getValueByField("classification");
    List<Map<String, String>> annotations = List<Map<String, String>> classification.get("annotations");
    String user_id = (String) classification.get("user_id");

    for(Map<String,String> annotation : annotations) {
      collector.emit(new Values(annotation.get("species"), user_id);
    }
  }
}
