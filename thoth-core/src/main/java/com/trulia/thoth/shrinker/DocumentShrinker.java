package com.trulia.thoth.shrinker;

import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.requestdocuments.MessageRequestDocument;
import com.trulia.thoth.util.Utils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * User: dbraga - Date: 11/7/14
 */
public class DocumentShrinker implements Callable{
  private static final Logger LOG = Logger.getLogger(DocumentShrinker.class);

  private HttpSolrServer shrankServer;
  private HttpSolrServer realTimeServer;
  private DateTime nowMinusTimeToShrink;
  private ServerDetail serverDetail;
  private static final String MASTER_MINUTES_DOCUMENT = "masterDocumentMin_b";
  public static final String SLOW_QUERY_DOCUMENT = "slowQueryDocument_b";
  private static final String MASTER_DOCUMENT_TIMESTAMP = "masterTime_dt";
  private static final String NAME_RANGE_0_10 = "range-0-10_i";
  private static final String NAME_RANGE_10_100 = "range-10-100_i";
  private static final String NAME_RANGE_100_1000 = "range-100-1000_i";
  private static final String NAME_RANGE_1000_OVER = "range-1000-OVER_i";
  private static final String NAME_TOT_COUNT = "tot-count_i";
  private static final String ZERO_HITS_QUERIES_COUNT = "zeroHits-count_i";
  private static final String EXCEPTION_QUERIES_COUNT = "exceptionCount_i";
  private static final String RANGE_QUERIES_COUNT = "RangeQueryCount_i";
  private static final String FACET_QUERIES_COUNT = "FacetQueryCount_i";
  private static final String PROPERTY_LOOKUP_QUERIES_COUNT = "PropertyLookupQueryCount_i";
  private static final String PROPERTY_HASH_LOOKUP_QUERIES_COUNT = "PropertyHashLookupQueryCount_i";
  private static final String GEOSPATIAL_QUERIES_COUNT = "GeospatialQueryCount_i";
  private static final String OPENHOMES_QURIES_COUNT = "OpenHomesQueryCount_i";
  private static final String AVG_QTIME = "avg_qtime_d";
  private static final String AVG_REQUESTS_IN_PROGRESS = "avg_requestsInProgress_d";

  private static final String VALUE_RANGE_0_10 = "0 TO 10";
  private static final String VALUE_RANGE_10_100 = "11 TO 100";
  private static final String VALUE_RANGE_100_1000 = "101 TO 1000";
  private static final String VALUE_RANGE_1000_OVER = "1001 TO *";
  private static final String VALUE_TOT_COUNT = "* TO *";

  private static final String BITMASK_CONTAINS_RANGE_QUERY ="1??????";
  private static final String BITMASK_CONTAINS_FACET_QUERY ="?1?????";
  private static final String BITMASK_CONTAINS_PROPERTY_LOOKUP_QUERY ="??1????";
  private static final String BITMASK_CONTAINS_PROPERTY_HASH_LOOKUP_QUERY ="???1???";
  private static final String BITMASK_CONTAINS_GEOSPATIAL_QUERY ="?????1?";
  private static final String BITMASK_CONTAINS_OPEN_HOMES_QUERY ="??????1";

  public static final String ID = "id";
  public static final String EXCEPTION = "exception_b";
  public static final String QTIME = "qtime_i";
  public static final String HITS = "hits_i";
  public static final String BITMASK = "bitmask_s";
  public static final String REQUESTS_IN_PROGRESS = "requestInProgress_i";

  private int MAX_NUMBER_SLOW_THOTH_DOCS = 10;

  public DocumentShrinker(ServerDetail serverDetail, DateTime nowMinusTimeToShrink, HttpSolrServer thothServer, HttpSolrServer thothShrankServer) {
    this.shrankServer = thothShrankServer;
    this.realTimeServer = thothServer;
    this.nowMinusTimeToShrink = nowMinusTimeToShrink;
    this.serverDetail = serverDetail;

  }

  /**
   * 1) Find thoth documents in the real time core
   * 2) Get information about those docs, like facets and stats
   * 3) Create Shrank summary document from them, and add the document to the shrank core
   * 4) Tag top slow thoth documents and add them to the shrank core
   * 5) Clean up real time core
   */

  @Override
  public Object call() throws Exception {
    try {
      QueryResponse thothDocumentsFacets = fetchFacets();
      if (isThereAnythingToShrink(thothDocumentsFacets)){
        // Something to shrink
        LOG.info("Found documents to shrink for server " + serverDetail.getName() + "("+serverDetail.getCore()+"):" + serverDetail.getPort());
        // Fetch some stats about the thoth documents
        QueryResponse thothDocumentsStats = fetchStats();
        // Prepare the shrank document
        SolrInputDocument shrankDocument = generateShrankDocument(thothDocumentsFacets, thothDocumentsStats);
        // Add the document to the index
        shrankServer.add(shrankDocument);
        // Tag slower request documents and add them to the shrank core
        tagAndAddSlowThothDocuments();
        // Clean real time core
        realTimeServer.deleteByQuery(getRealTimeCoreCleanUpQuery());
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
      return "Completed";
    }

  }


  /**
   * Creates a range query following Solr syntax
   * @param field field to be searched on
   * @param range desired range
   * @return
   */
  private String createRangeQuery(String field, String range){
    return field + ":[" + range + "]";
  }

  /**
   * Creates a field/value query folowing Solr Syntax
   * @param field field to be searched on
   * @param value desired value
   * @return
   */
  private String createFieldValueQuery(String field, String value){
    return field + ":" + value;
  }


  /**
   * Define facet query to fetch facet counts from Thoth docs
   * @return SolrQuery
   */
  private SolrQuery defineFacetQuery(){
    return new SolrQuery().
        setFacet(true).
        // Get counts on
        addFacetQuery(createRangeQuery(QTIME,VALUE_RANGE_0_10)).  // Qtime between 0 and 10 ms
        addFacetQuery(createRangeQuery(QTIME,VALUE_RANGE_10_100)).  // Qtime between 10 and 100 ms
        addFacetQuery(createRangeQuery(QTIME,VALUE_RANGE_100_1000)). // Qtime between 100 and 1000 ms
        addFacetQuery(createRangeQuery(QTIME,VALUE_RANGE_1000_OVER)). // Qtime over 1000
        addFacetQuery(createRangeQuery(QTIME,VALUE_TOT_COUNT)). // All Qtimes
        addFacetQuery(createFieldValueQuery(HITS, "0")). // 0 hits queries
        addFacetQuery(createFieldValueQuery("NOT " + HITS, "0")). // queries with hits > 0
        addFacetQuery(createFieldValueQuery(EXCEPTION, "true")). // exception documents
        addFacetQuery(createFieldValueQuery(BITMASK, BITMASK_CONTAINS_RANGE_QUERY)).   // range queries
        addFacetQuery(createFieldValueQuery(BITMASK, BITMASK_CONTAINS_FACET_QUERY)). // facet queries
        addFacetQuery(createFieldValueQuery(BITMASK, BITMASK_CONTAINS_PROPERTY_LOOKUP_QUERY)). // lookup queries
        addFacetQuery(createFieldValueQuery(BITMASK, BITMASK_CONTAINS_PROPERTY_HASH_LOOKUP_QUERY)).  // hash lookup queries
        addFacetQuery(createFieldValueQuery(BITMASK, BITMASK_CONTAINS_GEOSPATIAL_QUERY)). // geospatial queries
        addFacetQuery(createFieldValueQuery(BITMASK, BITMASK_CONTAINS_OPEN_HOMES_QUERY)). // open homes queries
        // Set normal query
        setQuery(createThothDocsAggregationQuery()).
        setRows(0);

  }

  private SolrQuery defineStatsQuery(){
    SolrQuery solrQuery = new SolrQuery().
        setQuery(createThothDocsAggregationQuery()).
        setRows(0);
    solrQuery.setGetFieldStatistics(QTIME);
    solrQuery.setGetFieldStatistics(REQUESTS_IN_PROGRESS);
    return solrQuery;
  }

  /**
   * Check if a field is present
   * @param field to check
   * @return true if is present
   */
  public boolean isFieldPresent(Object field){
    return (field != null);
  }


  /**
   * Generate query for to aggregate thoth docs
   * @return solr query
   */
  private String createThothDocsAggregationQuery(){ // look for every document that has
    return
        createRangeQuery(MessageRequestDocument.TIMESTAMP, Utils.dateTimeToZuluSolrFormat(nowMinusTimeToShrink) + " TO *") +   // timestamp between the interval provided (past) and now
        createFieldValueQuery(MessageRequestDocument.HOSTNAME,"\"" + serverDetail.getName() + "\"" ) +  // with the provided hostname
        " AND NOT " + createFieldValueQuery(MASTER_MINUTES_DOCUMENT ,"true ") + // that has not already shrank
        " AND NOT " + createFieldValueQuery(SLOW_QUERY_DOCUMENT ,"true") ; // and it's not a slow query document
  }

  /**
   * Determine if there is any data to shrink
   * @param rsp query response
   * @return true or false depending if there is any data to shrink
   */
  private boolean isThereAnythingToShrink(QueryResponse rsp){
    return (rsp.getResults().getNumFound() > 0) ;
  }

  /**
   * Fetch stats about the thoth documents
   * @return query response containing stats
   * @throws SolrServerException
   */

  public QueryResponse fetchStats() throws SolrServerException {
    return realTimeServer.query(defineStatsQuery());
  }


  /**
   * Fetch facets about the thoth documents
   * @return query response containing facets
   * @throws SolrServerException
   */
  public QueryResponse fetchFacets() throws SolrServerException {
    return  realTimeServer.query(defineFacetQuery());
  }


  /**
   * Generate a Shrank document given facets and stats
   * @param facets
   * @param stats
   * @return Shrank document
   */
  public SolrInputDocument generateShrankDocument(QueryResponse facets, QueryResponse stats){
     SolrInputDocument shrankDocument = new SolrInputDocument();

    if (isFieldPresent(stats.getFieldStatsInfo().get(QTIME))){  // Add QTime stats
      shrankDocument.addField(AVG_QTIME, stats.getFieldStatsInfo().get(QTIME).getMean());
    }

    if (isFieldPresent(stats.getFieldStatsInfo().get(REQUESTS_IN_PROGRESS))){  // Add number of queries on deck
      shrankDocument.addField(AVG_REQUESTS_IN_PROGRESS, stats.getFieldStatsInfo().get(REQUESTS_IN_PROGRESS).getMean());
    }


    shrankDocument.addField(MASTER_DOCUMENT_TIMESTAMP, Utils.dateTimeToZuluSolrFormat(nowMinusTimeToShrink));
    shrankDocument.addField(MASTER_MINUTES_DOCUMENT, true);

    // Information about the Solr instance
    shrankDocument.addField(MessageRequestDocument.HOSTNAME, serverDetail.getName());
    shrankDocument.addField(MessageRequestDocument.POOL, serverDetail.getPool());
    shrankDocument.addField(MessageRequestDocument.PORT, Integer.parseInt(serverDetail.getPort()));
    shrankDocument.addField(MessageRequestDocument.CORENAME, serverDetail.getCore());


    // QTime ranges
    shrankDocument.addField(NAME_RANGE_0_10, getValueFromFacet(facets, QTIME + ":[" + VALUE_RANGE_0_10 + "]"));
    shrankDocument.addField(NAME_RANGE_10_100, getValueFromFacet(facets, QTIME + ":[" + VALUE_RANGE_10_100 + "]"));
    shrankDocument.addField(NAME_RANGE_100_1000, getValueFromFacet(facets, QTIME + ":[" + VALUE_RANGE_100_1000 + "]"));
    shrankDocument.addField(NAME_RANGE_1000_OVER, getValueFromFacet(facets, QTIME + ":[" + VALUE_RANGE_1000_OVER + "]"));
    shrankDocument.addField(NAME_TOT_COUNT, getValueFromFacet(facets, QTIME + ":[" + VALUE_TOT_COUNT + "]"));

    // Zero hits queries
    shrankDocument.addField(ZERO_HITS_QUERIES_COUNT, getValueFromFacet(facets, HITS + ":0"));

    // Exception count
    shrankDocument.addField(EXCEPTION_QUERIES_COUNT, getValueFromFacet(facets, EXCEPTION + ":true"));

    // Type of queries
    // TODO: generalize
    shrankDocument.addField(RANGE_QUERIES_COUNT, getValueFromFacet(facets, BITMASK + ":" + BITMASK_CONTAINS_RANGE_QUERY));
    shrankDocument.addField(FACET_QUERIES_COUNT, getValueFromFacet(facets, BITMASK + ":" + BITMASK_CONTAINS_FACET_QUERY));
    shrankDocument.addField(PROPERTY_LOOKUP_QUERIES_COUNT, getValueFromFacet(facets, BITMASK + ":" + BITMASK_CONTAINS_PROPERTY_LOOKUP_QUERY));
    shrankDocument.addField(PROPERTY_HASH_LOOKUP_QUERIES_COUNT, getValueFromFacet(facets, BITMASK + ":" + BITMASK_CONTAINS_PROPERTY_HASH_LOOKUP_QUERY));
    shrankDocument.addField(GEOSPATIAL_QUERIES_COUNT, getValueFromFacet(facets, BITMASK + ":" + BITMASK_CONTAINS_GEOSPATIAL_QUERY));
    shrankDocument.addField(OPENHOMES_QURIES_COUNT, getValueFromFacet(facets, BITMASK + ":" + BITMASK_CONTAINS_OPEN_HOMES_QUERY));


    return shrankDocument;
  }

  /**
   * Tag slower documents and add them to the shrank core
   */
  private void tagAndAddSlowThothDocuments() throws IOException, SolrServerException {
    // Query to return top MAX_NUMBER_SLOW_THOTH_DOCS slower thoth documents
    QueryResponse qr = realTimeServer.query(
        new SolrQuery()
            .setQuery(createThothDocsAggregationQuery())
            .addSort(QTIME, SolrQuery.ORDER.desc)
            .setRows(MAX_NUMBER_SLOW_THOTH_DOCS)
    );

    for (SolrDocument solrDocument: qr.getResults()){
      SolrInputDocument si = ClientUtils.toSolrInputDocument(solrDocument);
      // Remove old ID and version
      si.removeField(ID);
      si.removeField("_version_");
      // Tag document as slow
      si.addField(SLOW_QUERY_DOCUMENT, true);
      LOG.debug("Adding slow query document for server " + serverDetail.getName());
      shrankServer.add(si);
    }
  }


  /**
   * Create real time core clean up query
   * @return clean up query
   */
  private String getRealTimeCoreCleanUpQuery(){
    // Remove the single documents
    return
        createFieldValueQuery(MessageRequestDocument.HOSTNAME, "\""+ serverDetail.getName() +"\"")
        + " AND " + createFieldValueQuery(MessageRequestDocument.CORENAME, "\""+ serverDetail.getCore() +"\"")
        + " AND " + createFieldValueQuery(MessageRequestDocument.PORT, "\""+ serverDetail.getPort() +"\"")
        + " AND NOT " + createFieldValueQuery(MASTER_MINUTES_DOCUMENT, "true")
        // Keep the exception documents for now
        + " AND NOT " + createFieldValueQuery(EXCEPTION, "true")
        + " AND NOT " + createFieldValueQuery(SLOW_QUERY_DOCUMENT, "true");
  }


  /**
   * Retrieve value from Facet
   * @param rsp query response
   * @param key field key
   * @return object value
   */
  private Object getValueFromFacet(QueryResponse rsp, String key){
    return rsp.getFacetQuery().get(key);
  }


}
