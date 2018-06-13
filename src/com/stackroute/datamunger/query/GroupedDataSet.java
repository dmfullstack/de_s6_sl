package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

/*
 * Processing queries with group by clause will result in GroupedDataSet which will 
 * contain multiple dataSets, each of them indexed with the key column. Hence, the 
 * structure has been taken as a subtype of HashMap<String,Object>
 */
public class GroupedDataSet extends LinkedHashMap<String, Object> {

}
