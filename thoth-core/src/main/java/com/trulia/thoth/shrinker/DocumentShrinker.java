package com.trulia.thoth.shrinker;

import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.util.Utils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * User: dbraga - Date: 11/7/14
 */
public class DocumentShrinker implements Callable{
  private static final Logger LOG = Logger.getLogger(DocumentShrinker.class);

  private HttpSolrServer shrankServer;
  private HttpSolrServer server;
  private DateTime nowMinusTimeToShrink;
  private ServerDetail serverDetail;
  private static final String MASTER_MINUTES_DOCUMENT = "masterDocumentMin_b";
  public static final String SLOW_QUERY_DOCUMENT = "slowQueryDocument_b";
  private static final String MASTER_DOCUMENT_TIMESTAMP = "masterTime_dt";

  public DocumentShrinker(ServerDetail serverDetail, DateTime nowMinusTimeToShrink, HttpSolrServer thothServer, HttpSolrServer thothShrankServer) {
    this.shrankServer = thothShrankServer;
    this.server = thothServer;
    this.nowMinusTimeToShrink = nowMinusTimeToShrink;
    this.serverDetail = serverDetail;

  }


  private SolrQuery defineFacetQuery(){
    return new SolrQuery().
        setFacet(true).
        // Get counts on
            addFacetQuery(SolrLoggingDocument.QTIME + ":[" + Configuration.VALUE_RANGE_0_10 + "]").  // Qtime between 0 and 10 ms
        addFacetQuery(SolrLoggingDocument.QTIME + ":["+ Configuration.VALUE_RANGE_10_100 +"]").  // Qtime between 10 and 100 ms
        addFacetQuery(SolrLoggingDocument.QTIME + ":["+ Configuration.VALUE_RANGE_100_1000 +"]"). // Qtime between 100 and 1000 ms
        addFacetQuery(SolrLoggingDocument.QTIME + ":["+ Configuration.VALUE_RANGE_1000_OVER +"]"). // Qtime over 1000
        addFacetQuery(SolrLoggingDocument.QTIME + ":["+ Configuration.VALUE_TOT_COUNT +"]"). // All Qtimes
        addFacetQuery(SolrLoggingDocument.HITS + ":0"). // 0 hits queries
        addFacetQuery("NOT " + SolrLoggingDocument.HITS + ":0"). // queries with hits > 0
        addFacetQuery(SolrLoggingDocument.EXCEPTION + ":true"). // exception documents
        addFacetQuery(SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_RANGE_QUERY). // range queries
        addFacetQuery(SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_FACET_QUERY). // facet queries
        addFacetQuery(SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_PROPERTY_LOOKUP_QUERY). // lookup queries
        addFacetQuery(SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_PROPERTY_HASH_LOOKUP_QUERY).  // hash lookup queries
        addFacetQuery(SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_GEOSPATIAL_QUERY). // geospatial queries
        addFacetQuery(SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_OPEN_HOMES_QUERY). // open homes queries

        setQuery(createAggregationQuery()).
        setRows(0);

  }

  private SolrQuery defineStatsQuery(){
    SolrQuery solrQuery = new SolrQuery().
        setQuery(createAggregationQuery()).
        setRows(0);
    solrQuery.setGetFieldStatistics(SolrLoggingDocument.QTIME);
    solrQuery.setGetFieldStatistics(SolrLoggingDocument.REQUESTS_IN_PROGRESS);
    return solrQuery;
  }

  public boolean isFieldPresent(Object o){
    return o != null;
  }


  private SolrInputDocument addStatsToSolrInputDocument(QueryResponse rsp, SolrInputDocument solrInputDocument){
    if (isFieldPresent(rsp.getFieldStatsInfo().get(SolrLoggingDocument.QTIME))){  // Add Qtime stats
      solrInputDocument.addField(Configuration.AVG_QTIME, rsp.getFieldStatsInfo().get(SolrLoggingDocument.QTIME).getMean());
    }

    if (isFieldPresent(rsp.getFieldStatsInfo().get(SolrLoggingDocument.REQUESTS_IN_PROGRESS))){  // Add number of queries on deck
      solrInputDocument.addField(Configuration.AVG_REQUESTS_IN_PROGRESS, rsp.getFieldStatsInfo().get(SolrLoggingDocument.REQUESTS_IN_PROGRESS).getMean());
    }

    return solrInputDocument;
  }


  private String createAggregationQuery(){ // look for every document that has
    return MessageDocument.TIMESTAMP + ":[" + Utils.dateTimeToZuluSolrFormat(nowMinusTimeToShrink) + " TO *] " +   // timestamp between the date provided (past) and now
        "AND " + MessageDocument.HOSTNAME + ":\"" + serverDetail.getName() + "\" " + // with the provided hostname
        "AND NOT " + MASTER_MINUTES_DOCUMENT + ":true " + // that has not already shrank
        "AND NOT " + SLOW_QUERY_DOCUMENT + ":true" ; // and it's not a slow query document
  }
  @Override
  public Object call() throws Exception {
    try {

      QueryResponse rsp = server.query(defineFacetQuery());

      if (rsp.getResults().getNumFound() > 0){
        // Got something to shrink
        LOG.info("Found stuff to shrink for server " + serverDetail.getName());

        SolrInputDocument solrInputDocument = new SolrInputDocument();
        QueryResponse qr = server.query(defineStatsQuery());

        // Add avg stats to the shrank document
        solrInputDocument = addStatsToSolrInputDocument(qr, solrInputDocument);
        // Add other stats to the shrank document
        solrInputDocument = generateShrankDocument(rsp, solrInputDocument);
        // Add the document to the index
        shrankServer.add(solrInputDocument);

        // Tag the top slow documents and add them to the index
        ArrayList<SolrInputDocument> solrInputDocuments = generateSlowQueryDocuments(server);
        for (SolrInputDocument si : solrInputDocuments){
          shrankServer.add(si);
        }

        // TODO : ENABLE
        checkForRedFlags(solrInputDocument);

        // Wipe out documents already used
        Sweeper sweeper = new Sweeper(server, createSweepingQuery());
        sweeper.sweep();

      } else {
        LOG.info("Nothing to shrink for server " + serverDetail.getName() +":"+serverDetail.getPort()+" core:"+serverDetail.getCore()+" ...");
      }

      LOG.info( serverDetail.getName() +":"+serverDetail.getPort()+" core:"+serverDetail.getCore()+ " shrinking completed.");

    } catch (SolrServerException e) {
      LOG.error(e);
    } catch (Exception e) {
      LOG.error(e);
    }
    finally {
      return "Run";
    }

  }

  private String createSweepingQuery(){
    // Remove the single documents
    return
        //MessageDocument.TIMESTAMP + ":[* TO "+ Utils.dateTimeToZuluSolrFormat(past) +"] " +
        // "AND " +
        MessageDocument.HOSTNAME + ":\""+ serverDetail.getName() +"\" " +
            "AND " + MessageDocument.CORENAME + ":\""+ serverDetail.getCore() +"\" " +
            "AND " + MessageDocument.PORT + ":\""+ serverDetail.getPort() +"\" " +
            "AND NOT " + MASTER_MINUTES_DOCUMENT + ":true " +
            // Keep the exception documents for now
            "AND NOT " + SolrLoggingDocument.EXCEPTION + ":true " +
            "AND NOT " + SLOW_QUERY_DOCUMENT + ":true";
  }

  public void checkForRedFlags(SolrInputDocument solrInputDocument) throws JMSException {
    //LOG.info("QTime monitor for " + serverName);
    //new QtimeMonitor(solrInputDocument, Configuration.ACTIVEMQ_QUEUE_THOTH_NAGIOS, Loader.getInstance().getConfig()).check();
    //LOG.info("Zero hits monitor for " + serverName);
    //new ZeroHitsMonitor(solrInputDocument, Configuration.ACTIVEMQ_QUEUE_THOTH_NAGIOS, Loader.getInstance().getConfig()).check();
    //LOG.info("Exception monitor for " + serverName);
    //new QtimeMonitor(solrInputDocument, Configuration.ACTIVEMQ_QUEUE_THOTH_NAGIOS, Loader.getInstance().getConfig()).check();
    LOG.info("Skipping ...");
  }

  private Object getValueFromFacet(QueryResponse rsp, String key){
    return rsp.getFacetQuery().get(key);
  }


  private ArrayList<SolrInputDocument> generateSlowQueryDocuments(SolrServer server) throws SolrServerException {
    ArrayList<SolrInputDocument> solrInputDocumentArrayList = new ArrayList<SolrInputDocument>();

    QueryResponse qr = server.query(
        new SolrQuery()
            .setQuery(createAggregationQuery())
            .addSort(SolrLoggingDocument.QTIME, SolrQuery.ORDER.desc)
            .setRows(10)
    );

    for (SolrDocument solrDocument: qr.getResults()){
      SolrInputDocument si = ClientUtils.toSolrInputDocument(solrDocument);
      si.removeField(SolrLoggingDocument.ID);
      si.removeField("_version_");
      //si.addField(SolrLoggingDocument.ID,System.currentTimeMillis()+"-sq-" + Math.random());
      si.addField(SLOW_QUERY_DOCUMENT, true);
      LOG.debug("Adding slow query document for server " + serverDetail.getName());
      //server.add(si);
      solrInputDocumentArrayList.add(si);
    }
    return solrInputDocumentArrayList;
  }

  private SolrInputDocument generateShrankDocument(QueryResponse rsp, SolrInputDocument solrInputDocument){
    //SolrInputDocument solrInputDocument = new SolrInputDocument();




    //TODO: uniquify this id
    //String id = System.currentTimeMillis()+"-ds-"+ Math.random();
    //LOG.info("Document shrank id: " + id);

    // New fields unique for the shank document
    //solrInputDocument.addField(ID, id);
    solrInputDocument.addField(MASTER_DOCUMENT_TIMESTAMP, Utils.dateTimeToZuluSolrFormat(nowMinusTimeToShrink));
    solrInputDocument.addField(MASTER_MINUTES_DOCUMENT, true);


    // Information about the solr server
    solrInputDocument.addField(MessageDocument.HOSTNAME, serverDetail.getName());
    solrInputDocument.addField(MessageDocument.POOL, serverDetail.getPool());
    solrInputDocument.addField(MessageDocument.PORT, Integer.parseInt(serverDetail.getPort()));
    solrInputDocument.addField(MessageDocument.CORENAME, serverDetail.getCore());


    // Ranges
    solrInputDocument.addField(Configuration.NAME_RANGE_0_10, getValueFromFacet(rsp, SolrLoggingDocument.QTIME + ":[" + Configuration.VALUE_RANGE_0_10 + "]"));
    solrInputDocument.addField(Configuration.NAME_RANGE_10_100, getValueFromFacet(rsp, SolrLoggingDocument.QTIME + ":[" + Configuration.VALUE_RANGE_10_100 + "]"));
    solrInputDocument.addField(Configuration.NAME_RANGE_100_1000, getValueFromFacet(rsp, SolrLoggingDocument.QTIME + ":[" + Configuration.VALUE_RANGE_100_1000 + "]"));
    solrInputDocument.addField(Configuration.NAME_RANGE_1000_OVER, getValueFromFacet(rsp, SolrLoggingDocument.QTIME + ":[" + Configuration.VALUE_RANGE_1000_OVER + "]"));
    solrInputDocument.addField(Configuration.NAME_TOT_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.QTIME + ":[" + Configuration.VALUE_TOT_COUNT + "]"));

    // Zero hits queries
    solrInputDocument.addField(Configuration.ZERO_HITS_QUERIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.HITS + ":0"));

    // Exception count
    solrInputDocument.addField(Configuration.EXCEPTION_QUERIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.EXCEPTION + ":true"));
    // avg_hits_i  !?

    // Type of queries
    solrInputDocument.addField(Configuration.RANGE_QUERIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_RANGE_QUERY));
    solrInputDocument.addField(Configuration.FACET_QUERIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_FACET_QUERY));
    solrInputDocument.addField(Configuration.PROPERTY_LOOKUP_QUERIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_PROPERTY_LOOKUP_QUERY));
    solrInputDocument.addField(Configuration.PROPERTY_HASH_LOOKUP_QUERIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_PROPERTY_HASH_LOOKUP_QUERY));
    solrInputDocument.addField(Configuration.GEOSPATIAL_QUERIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_GEOSPATIAL_QUERY));
    solrInputDocument.addField(Configuration.OPENHOMES_QURIES_COUNT, getValueFromFacet(rsp, SolrLoggingDocument.BITMASK + ":" + Configuration.CONTAINS_OPEN_HOMES_QUERY));

    return solrInputDocument;

  }
}
