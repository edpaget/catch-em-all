package catch_em_all;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import java.util.*;

public class ListAnimals extends ReducerAggregator<Set> {
  public Set<String> init() {
    return HashSet<String>;
  }

  public Set<String> reduce(Set<String> curr, TridentTuple t) {
    curr.add(t.getStringByField("animal"));
    return curr;
  }
}
