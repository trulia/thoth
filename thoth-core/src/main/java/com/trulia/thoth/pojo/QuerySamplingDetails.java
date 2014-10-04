package com.trulia.thoth.pojo;

/**
 * User: dbraga - Date: 7/23/14
 */
public class QuerySamplingDetails {
  private Details details;
  private Feature features;

  public Feature getFeatures() {
    return features;
  }

  public void setFeatures(Feature features) {
    this.features = features;
  }

  public Details getDetails() {
    return details;
  }

  public void setDetails(Details details) {
    this.details = details;
  }

  public static class Feature{
    private boolean isFacet;
    private boolean isCollapsingSearch;
    private boolean isGeospatialSearch;
    private boolean containsOpenHomes;
    private boolean isRangeQuery;

    public boolean isFacet() {
      return isFacet;
    }

    public void setFacet(boolean isFacet) {
      this.isFacet = isFacet;
    }

    public boolean isCollapsingSearch() {
      return isCollapsingSearch;
    }

    public void setCollapsingSearch(boolean isCollapsingSearch) {
      this.isCollapsingSearch = isCollapsingSearch;
    }

    public boolean isGeospatialSearch() {
      return isGeospatialSearch;
    }

    public void setGeospatialSearch(boolean isGeospatialSearch) {
      this.isGeospatialSearch = isGeospatialSearch;
    }

    public boolean isContainsOpenHomes() {
      return containsOpenHomes;
    }

    public void setContainsOpenHomes(boolean containsOpenHomes) {
      this.containsOpenHomes = containsOpenHomes;
    }

    public boolean isRangeQuery() {
      return isRangeQuery;
    }

    public void setRangeQuery(boolean isRangeQuery) {
      this.isRangeQuery = isRangeQuery;
    }
  }

  public static class Details {

    private int rows = 10;
    private int start = 0;
    private String query;
    private String filterQuery;
    private String sort;
    private boolean slowpool = false;
    private String collapseField;
    private String collapseDocFl;
    private String ghl;
    private String facetField;
    private String facetZeros;

    public String getFacetField() {
      return facetField;
    }

    public void setFacetField(String facetField) {
      this.facetField = facetField;
    }

    public String getFacetZeros() {
      return facetZeros;
    }

    public void setFacetZeros(String facetZeros) {
      this.facetZeros = facetZeros;
    }

    public String getGhl() {
      return ghl;
    }

    public void setGhl(String ghl) {
      this.ghl = ghl;
    }

    public String getCollapseField() {
      return collapseField;
    }

    public void setCollapseField(String collapseField) {
      this.collapseField = collapseField;
    }

    public String getCollapseDocFl() {
      return collapseDocFl;
    }

    public void setCollapseDocFl(String collapseDocFl) {
      this.collapseDocFl = collapseDocFl;
    }

    public boolean isSlowpool() {
      return slowpool;
    }

    public void setSlowpool(String slowpool) {
      this.slowpool = Boolean.parseBoolean(slowpool);
    }

    public String getSort() {
      return sort;
    }

    public void setSort(String sort) {
      this.sort = sort;
    }

    public String getQuery() {
      return query;
    }

    public void setQuery(String query) {
      this.query = query;
    }

    public String getFilterQuery() {
      return filterQuery;
    }

    public void setFilterQuery(String filterQuery) {
      this.filterQuery = filterQuery;
    }

    public int getStart() {
      return start;
    }

    public void setStart(String start) {
      this.start = Integer.parseInt(start);
    }
    public void setRows(String rows) {
      this.rows = Integer.parseInt(rows);
    }

    public int getRows() {
      return rows;
    }
  }



}

