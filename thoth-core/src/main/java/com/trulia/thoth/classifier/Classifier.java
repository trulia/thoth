package com.trulia.thoth.classifier;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga - Date: 9/10/13
 */
public class Classifier {
  private static final Logger LOG = Logger.getLogger(Classifier.class);

  private  String query;
  private ArrayList<String> ranges;

  private static Pattern rangePattern = Pattern.compile("\\w*:\\[(.*?TO.*?)\\]");
  private static Pattern facetPattern = Pattern.compile("facet=true");
  private static Pattern collapsingSearchPattern = Pattern.compile("collapse.field=");
  private static Pattern geospatialSearchPattern = Pattern.compile("!spatial");

  /*
   bitMask

   [0] = containsRangeQuery
   [1] = isFacetingSearch
   [2] = isCollapsingSearch
   [3] = isGeospatialSearch

*/
  public static final int BITMASK_SIZE = 4;

  private int[] bitMask = {0,0,0,0};

  private void latch(int position){
    bitMask[position] = 1;
  }

  public Classifier(String query){
    this.query = query;
  }

  private String bitmaskToString(){
    String sbm = "";
    for (int el: bitMask){
      sbm += String.valueOf(el);
    }
    return  sbm;
  }

  public String createBitMask(){
    ranges = extractMatches(rangePattern);
    if (ranges.size() != 0){
      latch(0);
    }

    if (checkForMatch(facetPattern)) latch(1);
    if (checkForMatch(collapsingSearchPattern)) latch(2);
    if (checkForMatch(geospatialSearchPattern)) latch(3);

    return bitmaskToString();
  }



  private  ArrayList<String> extractMatches(Pattern pattern){
    ArrayList<String> matches = new ArrayList<String>();
    try{
      Matcher matcher = pattern.matcher(query);


      while (matcher.find()){
        matches.add(matcher.group(1));
      }
      return matches;
    }
    catch (Exception e){
      LOG.error("Exception on query " + query +" " +e);
      return matches;
    }
  }

  private boolean checkForMatch(Pattern pattern){
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) return true;
    else return false;
  }




}
