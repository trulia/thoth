package com.trulia.thoth.requestdocuments;

import com.trulia.thoth.classifier.Classifier;
import com.trulia.thoth.util.QueryUtil;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

/**
 * Created by rshioramwar on 10/22/14.
 */
public class SolrQueryRequestDocument extends AbstractBaseRequestDocument {

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

  public static final String DATE = "date_dt";
  public static final String PARAMS = "params_s";
  public static final String QTIME = "qtime_i";
  public static final String STATUS = "status_s";
  public static final String HITS = "hits_i";
  public static final String BITMASK = "bitmask_s";
  public static final String SLOWQUERY_TAG = "slowQuery_b";
  public static final String SLOWQUERY_VALIDITY_PREDICTION = "isSlowQueryPredictionValid_b";


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


  @Override
  public void populateSolrInputDocument(SolrInputDocument solrInputDocument) {
    if (timestamp != null) solrInputDocument.addField(DATE, timestamp.toDate());
    if (params != null) solrInputDocument.addField(PARAMS, params);
    if (qtime != -1) solrInputDocument.addField(QTIME, qtime);
    solrInputDocument.addField(STATUS, "0");  //TODO: remove hard coded status
    if (hits != -1) solrInputDocument.addField(HITS, hits);
    String bitmask = new Classifier(this.getParams()).createBitMask();
    if (bitmask!= "-1") solrInputDocument.addField(BITMASK, bitmask);

    solrInputDocument.addField(SLOWQUERY_TAG, slowQueryTagPresent);
    solrInputDocument.addField(SLOWQUERY_VALIDITY_PREDICTION, slowQueryTagPresent == QueryUtil.isSlowQuery((int) qtime));
  }
}
