package com.melot.relation.dao;

import java.util.Map;

public interface ActorRelationDAO {
	
	boolean insertRoomadmin(Map<String, Object> map);
	
	boolean insertBlacklist(Map<String, Object> map);
	
	boolean removeBlacklist(Map<String, Object> map);
	
	boolean removeRoomadmin(Map<String, Object> map);
}
