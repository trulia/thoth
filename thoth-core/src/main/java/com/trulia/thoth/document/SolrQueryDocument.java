package com.trulia.thoth.document;

import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;

import java.util.Date;

/**
 * User: dbraga - Date: 12/1/13
 */
public class SolrQueryDocument extends MessageDocument{

  //TODO: remove?
  //public static final String ID = "id";
  //public static final String BODY = "body_s";


  public static final String DATE = "date_dt";
  public static final String PARAMS = "params_s";
  public static final String QTIME = "qtime_i";
  public static final String STATUS = "status_s";
  public static final String HITS = "hits_i";
  public static final String BITMASK = "bitmask_s";

  protected Date date;
  //private String messageType; // INFO - DEBUG - ERROR ?
  protected String logClass;
  protected String params;
  protected int hits = -1;
  protected String status = "-1";
  protected int qtime = -1;
  protected String bitmask = "-1";
  protected String body;




  public SolrQueryDocument(MessageDocument messageDocument){
    this.coreName = messageDocument.getCoreName();
    this.portNumber = messageDocument.getPortNumber();
    this.hostname = messageDocument.getHostname();
    this.messageType = messageDocument.getMessageType();
    this.pool = messageDocument.getPool();
    this.source = messageDocument.getSource();
  }



  public SolrInputDocument toSolrInputDocument(){
    SolrInputDocument solrInputDocument = super.toSolrInputDocument();
    //TODO: switch to a logic : if field exists, then add it
    if (date != null) solrInputDocument.addField(DATE, date);
    if (params != null) solrInputDocument.addField(PARAMS, params);
    if (qtime != -1) solrInputDocument.addField(QTIME, qtime);
    if (status != "-1") solrInputDocument.addField(STATUS, status);
    if (hits != -1) solrInputDocument.addField(HITS, hits);
    if (bitmask!= "-1") solrInputDocument.addField(BITMASK, bitmask);
    return  solrInputDocument;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public void setDate(DateTime d) {
    this.date = d.toDate();
  }

  public String getLogClass() {
    return logClass;
  }

  public void setLogClass(String logClass) {
    this.logClass = logClass;
  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }

  public int getHits() {
    return hits;
  }

  public void setHits(int hits) {
    this.hits = hits;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public int getQtime() {
    return qtime;
  }

  public void setQtime(int qtime) {
    this.qtime = qtime;
  }

  public String getBitmask() {
    return bitmask;
  }

  public void setBitmask(String bitmask) {
    this.bitmask = bitmask;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }
}
