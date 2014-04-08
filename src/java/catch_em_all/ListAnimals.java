package catch_em_all;

import backtype.storm.tuple.Values;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import java.util.*;

public class ListAnimals implements ReducerAggregator<Set<String>> {
  public Set<String> init() {
    return new HashSet<String>();
  }

  public Set<String> reduce(Set<String> curr, TridentTuple t) {
    curr.add(t.getStringByField("animal"));
    return curr;
  }
}
