package com.trulia.thoth.request;

import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

/**
 * User: dbraga - Date: 12/1/13
 */
public class SolrQueryRequest {


  /**
   * params of the query
   */
  private String params;
  /**
   * number of hits of the query
   */
  private int hits;
  /**
   * QTime of the query
   */
  private long qtime;
  /**
   * path e.g: select/
   */
  private String path;
  /**
   * webapp
   */
  private String webapp;
  /**
   * timestamp of the query
   */
  private DateTime timestamp;

  /**
   * Is this query identified as a slow query
   */
  private boolean slowQueryTagPresent;

  public boolean isSlowQueryTagPresent() {
    return slowQueryTagPresent;
  }

  public String getWebapp() {
    return webapp;
  }

  @JsonProperty("webapp")
  public void setWebapp(String webapp) {
    this.webapp = webapp;
  }
  public String getPath() {
    return path;
  }
  @JsonProperty("path")
  public void setPath(String path) {
    this.path = path;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }
  @JsonProperty("timestamp")
  public void setTimestamp(long millisec){
    this.timestamp = new DateTime(millisec);
  }
  public long getQtime() {
    return qtime;
  }
  @JsonProperty("qtime")
  public void setQtime(int qtime) {
    this.qtime = qtime;
  }
  public int getHits() {
    return hits;
  }
  @JsonProperty("hits")
  public void setHits(int hits) {
    this.hits = hits;
  }
  public String getParams() {
    return params;
  }
  @JsonProperty("params")
  public void setParams(String params) {
    this.params = params;
    //TODO: bring this signature outside
    slowQueryTagPresent = this.params.contains("slowPool=1");
  }


}
