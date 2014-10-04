package com.trulia.thoth.mappers;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * User: dbraga - Date: 3/2/14
 */
public class Deserializer {
  private ObjectMapper mapper;
  // Singleton for thoth connection
  private static Deserializer instance = new Deserializer();

  private Deserializer(){
    mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static Deserializer getInstance(){
    return instance;
  }

  public ObjectMapper getMapper(){
    return mapper;
  }



}
