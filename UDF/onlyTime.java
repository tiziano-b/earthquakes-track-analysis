package com.tiziano.bigdataproject;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/*
UDF to exctract time form a text
*/


public class onlyTime extends UDF {
  public Text evaluate(Text s) {
    if (s == null){
      return null; 
    }

    StringBuilder newString = new StringBuilder("");
    String inputText = s.toString().toLowerCase();
    boolean exit = false;
    boolean time = false;
    int position = 0;
    
    while (!exit) {
      try {
        char c = inputText.charAt(position);
        if (time && c == '.') {
          exit = true;
          continue;
        } 
        if (time && !exit) {
          // newString = String.valueOf(newString) + c;
          newString.append(c);
          position++;
          continue;
        } 
        if (c == 't' || c == 'T') {
          time = true;
          position++;
          continue;
        } 
        position++;

      } catch (Exception e) {
        exit = true;
      } 
    } 
    return new Text(newString);
  }
}
