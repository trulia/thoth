package com.trulia.thoth.pojo;

/**
 * User: dbraga - Date: 9/13/14
 */
public class QueryPojo {
  private String params;
  private String qtime;
  private String hits;
  private String bitmask;

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }

  public String getQtime() {
    return qtime;
  }

  public void setQtime(String qtime) {
    this.qtime = qtime;
  }

  public String getHits() {
    return hits;
  }

  public void setHits(String hits) {
    this.hits = hits;
  }

  public String getBitmask() {
    return bitmask;
  }

  public void setBitmask(String bitmask) {
    this.bitmask = bitmask;
  }
}
