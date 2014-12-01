package com.trulia.thoth.util;

import com.trulia.thoth.pojo.ServerDetail;

import java.util.ArrayList;

/**
 * User: dbraga - Date: 11/30/14
 */
public class IgnoredServers {
  private ArrayList<ServerDetail> ignoredServerDetails;

  /**
   * Helper class to convert string representation of server list to array list of server details
   * @param stringListOfIgnoredServers string representation of list of servers to ignore
   * format of the string:
   * multiple server details are separated by comma ,
   * each server detail is separated by semicolon ;
   * example:
   * name-of-the-host1;123456;name-of-the-core1;name-of-the-pool1,name-of-the-host2;123456;name-of-the-core2;name-of-the-pool2
   *
   *
   */
  private void convertStringListToArraylist(String stringListOfIgnoredServers){
    ignoredServerDetails = new ArrayList<ServerDetail>();
    for (String ignoredServer: stringListOfIgnoredServers.split(",")){
      String[] split = ignoredServer.split(";");
      if (split.length % 4 == 0) ignoredServerDetails.add(
          new ServerDetail(split[0], split[3], split[1], split[2])
      );
    }
  }

  /**
   * Check if server is part of the ignored servers
   * @param serverDetail server to check
   * @return true if ignored , false if not
   */
  public boolean isServerIgnored(ServerDetail serverDetail){
    for (ServerDetail toCheck: ignoredServerDetails){
      if ((toCheck.getName().equals(serverDetail.getName())) &&
          (toCheck.getCore().equals(serverDetail.getCore())) &&
          (toCheck.getPool().equals(serverDetail.getPool())) &&
          (toCheck.getPort().equals(serverDetail.getPort()))){
        return true;
      }
    }
    return false;
  }

  public ArrayList<ServerDetail> getIgnoredServersDetail(){
    return this.ignoredServerDetails;
  }

  /**
   * Encapsulate the concept of ignored servers that are currently intercepted on Thoth
   * @param ignoredServersString string representation of the ignored server list
   */
  public IgnoredServers(String ignoredServersString){
    convertStringListToArraylist(ignoredServersString);
  }

  public IgnoredServers(ArrayList<ServerDetail> ignoredServerDetails){
    this.ignoredServerDetails = ignoredServerDetails;
  }

}
