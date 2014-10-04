package com.trulia.thoth.message;

/**
 * User: dbraga - Date: 10/1/13
 */
public class QueueMessage {

  private String port;
  private String coreName;
  private String hostname;
  private String messageType;
  private String pool;

  // Name of the attributes of the message
  public static String PORT = "port";
  public static String CORENAME = "coreName";
  public static String HOSTNAME = "hostname";
  public static String MESSAGE_TYPE = "msgType";
  public static String POOL_NAME = "poolName";

  public static final String INFO = "INFO";
  public static final String ERROR = "ERROR";
  public static final String DEBUG = "DEBUG";


  /**
   * Decompose a log message from the queue or topic
   * @param port solr port, example 8983
   * @param coreName solr core name, example: test
   * @param hostname solr server hostname. example: localhost
   * @param messageType log level: info, debug, error
   */

  public QueueMessage(String port, String coreName, String hostname, String messageType, String pool){
    this.port = port;
    this.coreName = coreName;
    this.hostname = hostname;
    this.messageType = messageType;
    this.pool = pool;
  }

  public String getPort() {
    return port;
  }

  public String getCoreName() {
    return coreName;
  }

  public String getHostname() {
    return hostname;
  }

  public String getMessageType() {
    return messageType;
  }

  public String getPool() {
    return pool;
  }

  public void setPool(String pool) {
    this.pool = pool;
  }
}

