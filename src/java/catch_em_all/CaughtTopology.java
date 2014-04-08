package catch_em_all;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.utils.Utils;

import storm.trident.operation.builtin.MapGet;
import storm.trident.tuple.TridentTuple;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TransactionalTridentKafkaSpout;

import catch_em_all.KafkaJson;
import catch_em_all.SplitAnimals;
import catch_em_all.ListAnimals;

import java.util.*;

public class CaughtTopology {

  private final BrokerHosts brokers;

  public CaughtTopology(String zk) {
    brokers = new ZkHosts(zk);
  }

  public StormTopology build(LocalDRPC drpc) {
    TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokers, "classifications", "catch-em-all");
    kafkaConfig.scheme = new SchemeAsMultiScheme(new KafkaJson());
    TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
    TridentTopology topology = new TridentTopology();

    TridentState seenAnimals = topology.newStream("user-classifications", kafkaSpout)
      .shuffle()
      .each(new Fields("classification"), new FilterSerengeti())
      .each(new Fields("classification"), new SplitAnimals(), new Fields("animal", "user"))
      .groupBy(new Fields("user"))
      .persistentAggregate(new MemoryMapState.Factory(), new ListAnimals(), new Fields("animals"));

    topology.newDRPCStream("animals", drpc)
      .stateQuery(seenAnimals, new Fields("args"), new MapGet(), new Fields("animals"));

    return topology.build();
  }

  public static void main(String [] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(500);
    LocalCluster cluster = new LocalCluster();
    LocalDRPC drpc = new LocalDRPC();
    CaughtTopology topology = new CaughtTopology("33.33.33.10:2181");
    cluster.submitTopology("catch-em-all", conf, topology.build(drpc));
    while (true) {
      System.out.println("edpaget classified: " + drpc.execute("animals", "user_id"));
      Utils.sleep(1000);
    }
  }
}
