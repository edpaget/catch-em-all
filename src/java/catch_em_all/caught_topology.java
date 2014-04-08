package catch_em_all;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.spout.SchemeAsMultiScheme;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import storm.kafka.trident.TridentKakfkaConfig;
import storm.kafka.trident.TransactionalTridentKafkaSpout;

import catch_em_all.KafkaJson;
import catch_em_all.SplitAnimals;
import catch_em_all.ListAnimals;

import java.util.*;

public class TridentCatchEmAll {

  private final BrokerHosts brokers;

  public TridentCatchEmAll(String zk) {
    brokers = new ZkHosts(zk);
  }

  public static StormTopology buildTopology(drpc) {
    TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokers, "classifications", "catch-em-all");
    kafkaConfig.scheme = new SchemeAsMultiScheme(new KafkaJson());
    TransactionalTridentKafkaSpout kafkaSpout = new TransactionaTridentKafkaSpout(kafkaConfig);
    TridentTopology topology = new TridentTopology();

    TridentState seenAnimals = topology.newStream("kafka-classifications", kafkaSpout)
      .shuffle()
      .each(new Fields("classification"), new AnimalSplit, new Fields("animal", "user"))
      .groupBy(new Fields("user"))
      .presistentAggregate(new MemoryMapState.Factory(), new ListAnimals(), new Fields("animals"));

    topology.newDRPCStream("animals", drpc)
      .each(new Fields("args"), new Split(), new Fields("user"))
      .stateQuery(seenAmials, new Fields("user"), new MapGet(), new Fields("animals"));

    return topology.build();
  }

  public static void main(String [] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(500);
    LocalCluster cluster = new LocalCluster();
    LocalDRPC drpc = new LocalDRPC();
    TridentCatchEmAll topology = new TridentCatchEmAll("33.33.33.10:2181");
    cluster.submitTopology("catch-em-all", conf, buildTopology());
    while (true) {
      System.out.println("edpaget classified: " + drpc.execute("user_id").join(", "));
      Utils.sleep(1000);
    }
  }
}
