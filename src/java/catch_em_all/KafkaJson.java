package catch_em_all;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.spout.Scheme;

import java.util.*;
import java.io.UnsupportedEncodingException;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJson implements Scheme {
  public List<Object> deserialize(byte[] bytes) {
    try {
      return new Values(new ObjectMapper().readValue(new String(bytes, "UTF-8"), HashMap.class));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Fields getOutputFields() {
    return new Fields("classification");
  }
}
