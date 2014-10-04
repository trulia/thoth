package com.trulia.thoth.request;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga - Date: 12/17/13
 */
public class SolrShardedQueryRequest extends SolrQueryRequest{
  /**
   * User: dbraga - Date: 11/23/13
   */

  //private static Pattern shardParamPattern =
  private String shardParam;


  public void setShardParam(){
    Pattern pa = Pattern.compile("&ghl=([\\w]*)");
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


}
