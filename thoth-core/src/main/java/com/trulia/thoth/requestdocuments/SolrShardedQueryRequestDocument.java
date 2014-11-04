package com.trulia.thoth.requestdocuments;

import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rshioramwar on 10/23/14.
 */
public class SolrShardedQueryRequestDocument extends SolrQueryRequestDocument {

  //private static Pattern shardParamPattern =
  private String shardParam;
  private static final String SHARDS_INFO = "shardsInfo_s";
  private static final String SHARDS_PARAM = "shardsParam_s";

  public void setShardParam(){
    Pattern pa = Pattern.compile("&ghl=([\\w-]*)");
    String param = getParams();
    Matcher matcher = pa.matcher(param);
    while (matcher.find()){
      this.shardParam = matcher.group(1);
    }
  }

  public String getShardParam(){
    return this.shardParam;
  }
  private ArrayList<LinkedHashMap> shardsInfo;

  public ArrayList<LinkedHashMap>  getShardsInfo() {
    return shardsInfo;
  }

  @JsonProperty("shardsInfo")
  public void setShardsInfo(ArrayList<LinkedHashMap> shardsInfo) {
    this.shardsInfo = shardsInfo;
  }

  @Override
  public void populateSolrInputDocument(SolrInputDocument solrInputDocument) {
    super.populateSolrInputDocument(solrInputDocument);
    if (shardsInfo != null) solrInputDocument.addField(SHARDS_INFO, shardsInfo.toString());
    if (shardParam != null) solrInputDocument.addField(SHARDS_PARAM, shardParam);
  }
}
