package com.trulia.thoth.document;

import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * User: dbraga - Date: 12/17/13
 */
public class SolrShardedQueryDocument extends SolrQueryDocument {
  private static ArrayList<LinkedHashMap> shardsInfo;
  private static final String SHARDS_INFO = "shardsInfo_s";
  private static String shardsParam;
  private static final String SHARDS_PARAM = "shardsParam_s";

  public SolrInputDocument toSolrInputDocument(){
    SolrInputDocument solrInputDocument = super.toSolrInputDocument();
    if (shardsInfo != null) solrInputDocument.addField(SHARDS_INFO, shardsInfo.toString());
    if (shardsParam != null) solrInputDocument.addField(SHARDS_PARAM, shardsParam);
    return  solrInputDocument;
  }

  public SolrShardedQueryDocument(SolrQueryDocument solrQueryDocument){
    super(solrQueryDocument);
    this.date = solrQueryDocument.getDate();
    this.logClass = solrQueryDocument.getLogClass();
    this.params = solrQueryDocument.getParams();
    this.hits = solrQueryDocument.getHits();
    this.status = solrQueryDocument.getStatus();
    this.qtime = solrQueryDocument.getQtime();
    this.bitmask = solrQueryDocument.getBitmask();
    this.body = solrQueryDocument.getBody();
    this.source = solrQueryDocument.getSource();
  }

  public Object  getShardsInfo() {
    return shardsInfo;
  }

  public void setShardsParam(String shardsParam){
    this.shardsParam = shardsParam;
  }
  public void setShardsInfo(ArrayList<LinkedHashMap> shardsInfo) {
    this.shardsInfo = shardsInfo;
  }


}
