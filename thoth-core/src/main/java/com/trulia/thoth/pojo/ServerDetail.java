package com.trulia.thoth.pojo;

/**
 * User: dbraga - Date: 7/21/14
 */
public class ServerDetail {
  private String name;
  private String pool;
  private String port;
  private String core;

  public ServerDetail(String name, String pool, String port, String core) {
    this.name = name;
    this.pool = pool;
    this.port = port;
    this.core = core;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPool() {
    return pool;
  }

  public void setPool(String pool) {
    this.pool = pool;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getCore() {
    return core;
  }

  public void setCore(String core) {
    this.core = core;
  }
}
