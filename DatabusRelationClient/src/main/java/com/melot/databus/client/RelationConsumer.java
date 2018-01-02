package com.melot.databus.client;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.melot.relation.dao.UserRelationDAO;

@Component
public class RelationConsumer extends AbstractDatabusCombinedConsumer
{
  public static final Logger LOG = Logger.getLogger(RelationConsumer.class.getName());

  @Autowired
  private UserRelationDAO userRelationDao;
  
  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent event,
                                            DbusEventDecoder eventDecoder)
  {
    return processEvent(event, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent event,
                                                 DbusEventDecoder eventDecoder)
  {
    return processEvent(event, eventDecoder);
  }

  private ConsumerCallbackResult processEvent(DbusEvent event,
                                              DbusEventDecoder eventDecoder)
  {
    GenericRecord decodedEvent = eventDecoder.getGenericRecord(event, null);
    LOG.info("consume");
    try {
      int userId = (Integer) decodedEvent.get("user_id");
      int followedId = (Integer) decodedEvent.get("followed_id");
      Long dtime = (Long) decodedEvent.get("add_time");
      String op_type = ((Utf8) decodedEvent.get("op_type")).toString();
      LOG.info("userid: " + userId +
               ", nickname: " + followedId +
               ", add_time: " + dtime + 
               ", op_type: " + op_type);
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("userId", userId);
      map.put("followedId", followedId);
      if (dtime != null) {
          map.put("dtime", new Date(dtime));
      }
      if (op_type.equals("insert")) {
    	  userRelationDao.addFollow(map);
      } else if (op_type.equals("delete")) {
    	  userRelationDao.removeFollow(map);
      } else {
    	  System.out.println("TODO : " + op_type);
      }
    } catch (Exception e) {
      LOG.error("error decoding event ", e);
      return ConsumerCallbackResult.ERROR;
    }
    return ConsumerCallbackResult.SUCCESS;
  }

}
