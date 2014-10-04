package com.trulia.thoth.quartz;

import com.trulia.thoth.pojo.QuerySamplingDetails;
import com.trulia.thoth.pojo.ServerDetail;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga - Date: 7/21/14
 */
public class SamplerWorker implements Callable<String>{
  private ServerDetail server;
  private BufferedWriter writer;
  private HttpSolrServer thothIndex;
  private ObjectMapper mapper;
  private String fileName;

  private String hostname;
  private String port;
  private String core;
  private String pool;
  // This is the original list, commented out because we don't need all of them right now
  //private static final String[] samplingFields = { "hostname_s", "port_i", "pool_s", "source_s", "params_s", "qtime_i", "hits_i", "bitmask_s", "timestamp_dt"};
  private static final String[] samplingFields = { "hostname_s", "pool_s", "source_s", "params_s", "qtime_i", "hits_i", "bitmask_s"};

  private static final Pattern FACET_PATTERN = Pattern.compile("facet=true");

  private static Pattern RANGE_QUERY_PATTERN = Pattern.compile("\\w*:\\[(.*?TO.*?)\\]");
  private static Pattern COLLAPSING_SEARCH_PATTERN = Pattern.compile("collapse.field=");
  private static Pattern GEOSPATIAL_PATTERN = Pattern.compile("!spatial");
  private static Pattern OPEN_HOMES_PATTERN = Pattern.compile("ohDay_ms:\\[");


  public static <T> List<T> randomSample(List<T> items, int m){
    Random rnd = new Random();

    for(int i=0;i<m;i++){
      int pos = i + rnd.nextInt(items.size() - i);
      T tmp = items.get(pos);
      items.set(pos, items.get(i));
      items.set(i, tmp);
    }
    return items.subList(0, m);
  }

  public SamplerWorker(ServerDetail server, String samplingDirectory, ObjectMapper mapper) throws IOException {
    this.server = server;
    this.mapper = mapper;
    DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
    Date date = new Date();
    this.fileName =  samplingDirectory + dateFormat.format(date) + "_" + server.getName();
    writer = new BufferedWriter(new FileWriter(new File(fileName), true));
    this.hostname = server.getName();
    this.pool = server.getPool();
    this.core = server.getCore();
    this.port = server.getPort();
    thothIndex = new HttpSolrServer("http://thoth:8983/solr/collection1");
  }

  public String writeValueOrEmptyString(SolrDocument doc, String fieldName){
    if (doc.containsKey(fieldName)) return doc.getFieldValue(fieldName).toString();
    else return "";
  }

  private boolean checkForMatch(Pattern pattern, String query){
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) return true;
    else return false;
  }

  public String extractDetailsFromParams(String params) throws IOException {
    String[] splitted = params.replaceAll("\\{","").replaceAll("\\}", "").split("&");
    if (splitted.length < 1) return "";

    QuerySamplingDetails querySamplingDetails = new QuerySamplingDetails();

    QuerySamplingDetails.Details details = new QuerySamplingDetails.Details();
    QuerySamplingDetails.Feature features = new QuerySamplingDetails.Feature();

    features.setFacet(checkForMatch(FACET_PATTERN, params));
    features.setCollapsingSearch(checkForMatch(COLLAPSING_SEARCH_PATTERN, params));
    features.setContainsOpenHomes(checkForMatch(OPEN_HOMES_PATTERN, params));
    features.setGeospatialSearch(checkForMatch(GEOSPATIAL_PATTERN, params));
    features.setRangeQuery(checkForMatch(RANGE_QUERY_PATTERN, params));


    for (String s : splitted){
      if ("".equals(s)) continue;
      String[] elements = s.split("=");
      if (elements.length == 2) {
        String k = elements[0];
        String v = elements[1];
        if ("start".equals(k)) details.setStart(v);
        else if ("rows".equals(k)) details.setRows(v);
        else if ("q".equals(k)) details.setQuery(v);
        else if ("fq".equals(k)) details.setFilterQuery(v);
        else if ("sort".equals(k)) details.setSort(v);
        else if ("slowpool".equals(k)) details.setSlowpool(v);
        else if ("collapse.field".equals(k)) details.setCollapseField(v);
        else if ("collapse.includeCollapsedDocs.fl".equals(k)) details.setCollapseDocFl(v);
        else if ("facet.field".equals(k)) details.setFacetField(v);
        else if ("facet.zeros".equals(k)) details.setFacetZeros(v);
        else if ("ghl".equals(k)) details.setGhl(v);


        else if ( !("cachebust".equals(k)) && !("wt".equals(k)) && !("version".equals(k)) && !("version".equals(k)) && !("fl".equals(k)) ) {
            // want to know what i'm missing
            System.out.println("Missing field (" +k+ ")  value ("+v+")");
          }

      } else if (details.getQuery() == null && features.isGeospatialSearch() && s.contains("spatial")){
          elements = s.split("!spatial ");
          details.setQuery(elements[1]);
        }
      else System.out.println("Not recognized k,v element. from " + s);
      }



    querySamplingDetails.setDetails(details);
    querySamplingDetails.setFeatures(features);
    //System.out.println("JSON: " + mapper.writeValueAsString(querySamplingDetails));
    return mapper.writeValueAsString(querySamplingDetails);

  }


  @Override
  public String call() throws Exception {
    SolrQuery solrQuery = new SolrQuery("hostname_s:"+hostname+" AND port_i:"+port+" AND pool_s:"+pool+" AND coreName_s:"+core+" AND NOT exception_b:true" );
    solrQuery.setSort(new SolrQuery.SortClause("timestamp_dt", SolrQuery.ORDER.desc));
    solrQuery.setRows(100);  // Returning 100 docs
    QueryResponse qr = thothIndex.query(solrQuery);
    SolrDocumentList solrDocumentList = qr.getResults();

    if (solrDocumentList.size() < 1){
      System.out.println("ERROR: hostname: " + hostname+" returned 0 results. Skipping sampling" );
      writer.close();
      System.out.println(fileName + " closed.") ;
      return "skipped";
    }


    List<SolrDocument> sample = randomSample(solrDocumentList, 10); //Sampling 10
    for (SolrDocument doc: sample){
      for (String fieldName: samplingFields){

        if ("params_s".equals(fieldName)){
          String extractedDetails = extractDetailsFromParams(writeValueOrEmptyString(doc, fieldName));
          if (!"".equals(extractedDetails)) writer.write(extractedDetails);
        } else {
          writer.write(writeValueOrEmptyString(doc,fieldName));
        }
        writer.write("\t");

      }
      writer.write("\n");
    }

    writer.close();
    System.out.println(fileName +" closed.") ;

    return "done";
  }

  public static void main(String[] args) throws IOException {
    String params = "q={!spatial circles=33.6942,-112.033,1}(asmtBuildingArea_i:[ 2000 TO * ] ) AND (latitude_f:[ 33.27584 TO 34.06266 ]) AND (longitude_f:[ -112.32953 TO -111.91321 ])&sort=lastSaleDate_s desc&ghl=9w0sn3wx9c7-9mzg4bbgesf&fq=propertyId_s:[* TO *]&fq=lastSaleDate_s:[\"2013-10-09\" TO *]&fq=lastSalePrice_i:[1 TO *]&version=2.2&start=15&rows=15&cachebust=NOVERSION&wt=json&slowpool=1";
    ObjectMapper om  = new ObjectMapper();
    SamplerWorker samplerWorker = new SamplerWorker(new ServerDetail("","","",""),"", om);
    System.out.println(samplerWorker.extractDetailsFromParams(params));

  }
}
