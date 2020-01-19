package com.tiziano.bigdataproject;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class cleanLocation extends UDF {
  public Text evaluate(Text s) {
    if (s == null){
        return null;
    }
    
    String location =  s.toString();
    String newLocation = location.replaceAll(",", "-");
    return Text(newLocation);
    }
}
