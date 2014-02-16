/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bigtop.bigpetstore.util;

import java.util.ArrayList; 
/**
 * Borrowed from apache .  makes lib management in cluster easier.
 * Will add methods from http://grepcode.com/file_/repo1.maven.org/maven2/commons-lang/commons-lang/2.3/org/apache/commons/lang/StringUtils.java/?v=source
 * as necessary.
 */
public class StringUtils {
     
     public static String substringBefore(String str, String separator) {
         int pos = str.indexOf(separator);
         if (pos == -1) {
             return str;
         }
         return str.substring(0, pos);
     }


     public static String substringAfter(String str, String separator) {
         if (str.length()==0) {
             return str;
         }
         if (separator == null) {
             return "";
         }
         int pos = str.indexOf(separator);
         if (pos == -1) {
             return "";
         }
         return str.substring(pos + separator.length());
     }
 }
