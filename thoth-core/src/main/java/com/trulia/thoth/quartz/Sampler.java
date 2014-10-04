package com.trulia.thoth.quartz;

import com.trulia.thoth.pojo.ServerDetail;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.quartz.*;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * User: dbraga - Date: 7/21/14
 */
public class Sampler implements Job {
  private static final Logger LOG = Logger.getLogger(Sampler.class);
  private List<ServerDetail> serversDetail = null;
  private List<Future> futureList;
  private String mergeDirectory;
  private String samplingDirectory;
  private ObjectMapper mapper;
  String serverDetails;


  public ArrayList<File> filesinDir(String directoryName){
    ArrayList<File> filelist = new ArrayList<File>();
    File directory = new File(directoryName);
    //get all the files from a directory
    File[] fList = directory.listFiles();
    for (File file : fList){
      if (file.isFile()){
        filelist.add(file.getAbsoluteFile());
      }
    }
    return filelist;
  }


  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    SchedulerContext schedulerContext = null;
    try {
      mapper = new ObjectMapper();
      mapper.configure(SerializationConfig.Feature.WRITE_NULL_PROPERTIES, false);
      schedulerContext = context.getScheduler().getContext();
      mergeDirectory = (String)schedulerContext.get("mergingDir");
      samplingDirectory = (String)schedulerContext.get("samplingDir");
      serverDetails = (String)schedulerContext.get("serverDetails");

      serversDetail = new ArrayList<ServerDetail>();
      for (String server: serverDetails.trim().split(";")){
        String[] detail = server.trim().split(",");
        serversDetail.add(
                new ServerDetail(
                        detail[0],
                        detail[1],
                        detail[2],
                        detail[3]
                )
        );
      }

      ExecutorService service = Executors.newFixedThreadPool(10);
      futureList = new ArrayList<Future>();
      CompletionService<String> ser = new ExecutorCompletionService<String>(service);






      File dir = new File(samplingDirectory);
      new File(mergeDirectory).mkdirs();

      boolean success = (dir).mkdirs();
      if (success || dir.exists() ) {

        for (ServerDetail server: serversDetail){
          try {
            Future<String> future = ser.submit(new SamplerWorker(
                    server,
                    samplingDirectory,
                    mapper));
            futureList.add(future);
          } catch (IOException e) {
            e.printStackTrace();
          }

        }



        for(Future<String> fut : futureList){
          try {
            fut.get();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }





        LOG.info("Done Sampling. Merging single files into one");
        try {
          DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
          Date date = new Date();
          String mergeFile = mergeDirectory +  dateFormat.format(date)+"_merged";
          BufferedWriter bufferedWriter =  new BufferedWriter((new OutputStreamWriter(new FileOutputStream( mergeFile, true),"UTF-8")));


          for (File file: filesinDir(samplingDirectory)){
            LOG.info("Merging file: "  + file.getAbsoluteFile()+" to " + mergeFile);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
              bufferedWriter.write(line);
              bufferedWriter.write("\n");
            }
            reader.close();
            LOG.info("Merge finished. Deleting file: " + file.getAbsoluteFile() );
            file.delete();

          }
          bufferedWriter.close();



          LOG.info("Merge complete.");
        } catch (IOException e) {
          e.printStackTrace();
        }


      } else {
        LOG.error("Coudn't create directory");
      }

    } catch (SchedulerException e) {
      e.printStackTrace();
    }

  }

}
