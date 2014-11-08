package com.trulia.thoth.shrinker;

import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.util.ThothServers;
import com.trulia.thoth.util.Utils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.joda.time.DateTime;
import org.quartz.*;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * User: dbraga - Date: 11/7/14
 */
public class ShrinkerJob implements Job {
  private static final Logger LOG = Logger.getLogger(ShrinkerJob.class);
  private static DateTime nowMinusTimeToShrink;
  private String thothIndexUrl;
  private int shrinkPeriod;
  private int threadPoolSize = 1;

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    SchedulerContext schedulerContext = null;
    try {
      schedulerContext = context.getScheduler().getContext();
      shrinkPeriod = (Integer) schedulerContext.get("period");
      thothIndexUrl = (String) schedulerContext.get("thothIndexUrl");

      nowMinusTimeToShrink = new DateTime().minusSeconds(shrinkPeriod);

      LOG.info("Shrinking documents between " + Utils.dateTimeToZuluFormat(nowMinusTimeToShrink) + " and " + Utils.dateTimeToZuluFormat(new DateTime()));
      createShrinkers();

    } catch (SchedulerException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    } catch (ExecutionException e) {
      LOG.error(e);
    } catch (SolrServerException e) {
      LOG.error(e);
    }
  }










  public void createShrinkers() throws SolrServerException, InterruptedException, ExecutionException {
    ExecutorService service = Executors.newFixedThreadPool(threadPoolSize);
    CompletionService<String> ser = new ExecutorCompletionService<String>(service);

    HttpSolrServer thothServer = new HttpSolrServer(thothIndexUrl + "collection1/");
    HttpSolrServer thothShrankServer = new HttpSolrServer(thothIndexUrl + "shrank/");


    ThothServers thothServers  = new ThothServers();
    ArrayList<ServerDetail> listOfServers = thothServers.getList(thothServer);

    for (ServerDetail serverDetail: listOfServers){
      LOG.info("Shrinking docs for server("+serverDetail.getName()+"):("+serverDetail.getPort()+") ");
      ser.submit(new DocumentShrinker(serverDetail, nowMinusTimeToShrink, thothServer, thothShrankServer));
    }


    for(int i = 0; i < listOfServers.size(); i++){
      String result = ser.take().get();
    }
    System.out.println("Done");
    //System.out.println("Shutting down service");
    //service.shutdown();
    //System.exit(0);
  }



}
