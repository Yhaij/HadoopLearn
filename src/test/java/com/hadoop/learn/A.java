package com.hadoop.learn;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingDeque;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/9/22.
 */
public class A {
    private static String[] getPathStrings(String commaSeparatedPaths) {
        int length = commaSeparatedPaths.length();
        int curlyOpen = 0;
        int pathStart = 0;
        boolean globPattern = false;
        List<String> pathStrings = new ArrayList<String>();

        for (int i=0; i<length; i++) {
            char ch = commaSeparatedPaths.charAt(i);
            switch(ch) {
                case '{' : {
                    curlyOpen++;
                    if (!globPattern) {
                        globPattern = true;
                    }
                    break;
                }
                case '}' : {
                    curlyOpen--;
                    if (curlyOpen == 0 && globPattern) {
                        globPattern = false;
                    }
                    break;
                }
                case ',' : {
                    if (!globPattern) {
                        pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
                        pathStart = i + 1 ;
                    }
                    break;
                }
                default:
                    continue; // nothing special to do for this character
            }
        }
        pathStrings.add(commaSeparatedPaths.substring(pathStart, length));

        return pathStrings.toArray(new String[0]);
    }
    public static void main(String[] args) {
        String[] aa = getPathStrings("hdfs://10.1.13.111:8020/user{fas,fas},hdfs://10.1.13.111:8020/yhj");
        System.out.println(aa.length);
        System.out.println(Arrays.toString(aa));
    }
}
