package com.melot.databus.client;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.melot.relation.dao.ActorRelationDAO;

@Component
public class ActorRelationConsumer extends AbstractDatabusCombinedConsumer {
	public static final Logger log = Logger.getLogger(ActorRelationConsumer.class
			.getName());
	
	private AtomicInteger count = new AtomicInteger(0);
	
	@Autowired
	private ActorRelationDAO actorRelationDAO;

	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent event,
			DbusEventDecoder eventDecoder) {
		return processEvent(event, eventDecoder);
	}

	@Override
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent event,
			DbusEventDecoder eventDecoder) {
		return processEvent(event, eventDecoder);
	}

	private ConsumerCallbackResult processEvent(DbusEvent event,
			DbusEventDecoder eventDecoder) {
		GenericRecord decodedEvent = eventDecoder.getGenericRecord(event, null);
		log.info("consume");
		try {
			int userId = (Integer) decodedEvent.get("actor_id");
			int followedId = (Integer) decodedEvent.get("relation_id");
			Integer type = (Integer) decodedEvent.get("type");
			Integer operatorId = (Integer) decodedEvent.get("operator_id");
			Long create_time = (Long) decodedEvent.get("create_time");
			Long end_time = (Long) decodedEvent.get("end_time");
			Long operator_time = (Long) decodedEvent.get("operator_time");
			String extend_data = ((Utf8) decodedEvent.get("extend_data")).toString();
			String op_type = ((Utf8) decodedEvent.get("op_type")).toString();
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("actorId", userId);
			map.put("followedId", followedId);
			map.put("type", type);
			if (operatorId != null) {
				map.put("operatorId", operatorId);
			}
			if (create_time != null) {
				map.put("createTime", new Date(create_time));
			}
			if (end_time != null) {
				map.put("endTime", new Date(end_time));
			}
			if (operator_time != null) {
				map.put("operatorTime", new Date(operator_time));
			}
			if (extend_data != null) {
				map.put("extendData", extend_data);
			}
			log.info("event msg is : " + new Gson().toJson(map) + ", 数量: " + count.incrementAndGet());
			if (op_type.equals("delete")) {
				if (type.equals(1)) {
					actorRelationDAO.removeBlacklist(map);
				} else if (type.equals(3)) {
					actorRelationDAO.removeRoomadmin(map);
				}
			} else if (op_type.equals("insert")) {
				if (type.equals(1)) {
					actorRelationDAO.insertBlacklist(map);
				} else if (type.equals(3)) {
					actorRelationDAO.insertRoomadmin(map);
				} else {
					log.warn("no support relation type is : " + type);
				}
			} else {
				log.warn("no support op_type is : " + op_type);
			}
		} catch (Exception e) {
			log.error("error decoding event ", e);
			return ConsumerCallbackResult.ERROR;
		}
		return ConsumerCallbackResult.SUCCESS;
	}
}
