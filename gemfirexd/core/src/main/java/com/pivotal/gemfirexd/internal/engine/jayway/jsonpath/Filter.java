/*
 * Copyright 2011 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pivotal.gemfirexd.internal.engine.jayway.jsonpath;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class Filter implements Predicate {

    private List<Criteria> criteriaList = new ArrayList<Criteria>();
    private boolean isAndCriteria;
    //private boolean isOrCriteria;
    
    private Filter(Criteria criteria) {
        this.criteriaList.add(criteria);
    }

    private Filter(List<Criteria> criteriaList, boolean isAndCriteria, boolean isOrCriteria) {
        this.criteriaList = criteriaList;
        this.isAndCriteria = isAndCriteria;
        //this.isOrCriteria = isOrCriteria;
    }

    public static Filter filter(Criteria criteria) {
        return new Filter(criteria);
    }

    public static Filter filter(List<Criteria> criteriaList, boolean isAndCriteria, boolean isOrCriteria) {
        return new Filter(criteriaList, isAndCriteria, isOrCriteria);
    }

    @Override
    public boolean apply(Object target, Configuration configuration) {
      if(isAndCriteria){
        for (Criteria criteria : criteriaList) {
            if (!criteria.apply(target, configuration)) {
                return false;
            }
        }
        return true;
      }
      
      for (Criteria criteria : criteriaList) {
         if (criteria.apply(target, configuration)) {
              return true;
          }
      }
      return false;
    }

    public void addCriteria(Criteria criteria) {
        criteriaList.add(criteria);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Criteria crit : criteriaList) {
            sb.append(crit.toString());
        }
        return sb.toString();
    }
}
