package com.tiziano.bigdataproject;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/*
UDF to exctract date form text
*/

public class onlyDate extends UDF {
  public Text evaluate(Text s) {

    if (s == null){
      return null; 
    }

    StringBuilder newString = new StringBuilder();
    String inputText = s.toString().toLowerCase();
    boolean exit = false;
    int position = 0;

    while (!exit) {
      try {
        char c = inputText.charAt(position);
        if (c == 't' || c == 'T') {
          exit = true;
          continue;
        }
        newString.append(c);
        position++;
      } catch (Exception e) {
        exit = true;
      } 
    } 
    return new Text(newString);
  }
}
