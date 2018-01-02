package com.melot.relation.dao;

import java.util.Map;

public interface UserRelationDAO {
	
	boolean addFollow(Map<String, Object> map);
	
	boolean removeFollow(Map<String, Object> map);
	
}
