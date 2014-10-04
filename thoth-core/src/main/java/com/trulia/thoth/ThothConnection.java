package com.trulia.thoth;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class ThothConnection {

  // Singleton for thoth connection
  private static ThothConnection instance = new ThothConnection();
  private SolrServer server;
  private String thothIndexURL;

  //private ThothConnection(){
  //  server = new HttpSolrServer(thothIndexURL);
  //}

  public static ThothConnection getInstance(){
    return instance;
  }

  public SolrServer getServer(){
    return this.server;
  }


  public void setThothIndexURL(String thothIndexURL) {
    this.thothIndexURL = thothIndexURL;
  }

  public String getThothIndexURL() {
    return thothIndexURL;
  }

  public void init() {
    server = new HttpSolrServer(thothIndexURL);
  }
}
