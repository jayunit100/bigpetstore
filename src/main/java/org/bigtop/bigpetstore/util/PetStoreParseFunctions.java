package org.bigtop.bigpetstore.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO : use this in pig etc///
 */
public class PetStoreParseFunctions {
    
    String[] headers = {"code","city","country","lat","lon"} ;
    
    public Map<String, Object> parse(String line)  {
    
        Map<String, Object> resultMap = new HashMap<String, Object>();
    
        List<String> csvObj = null;
    
        String[] temp = line.split(",");
        csvObj = new ArrayList<String>(Arrays.asList(temp));
    
        if (csvObj.isEmpty()) {
            return resultMap;
        }
    
        int k = 0;
    
        for (String valueStr : csvObj) {
    
          resultMap.put(headers[k++], valueStr);
    
        }
    
        return resultMap;
    }
}

