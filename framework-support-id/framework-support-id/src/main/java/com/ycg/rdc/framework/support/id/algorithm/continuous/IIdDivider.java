package com.ycg.rdc.framework.support.id.algorithm.continuous;

public interface IIdDivider {
	
	Boolean tryToCreateScope(String scopeName, String schemeName, String fieldName);
	
	Boolean tryToCreateScope(String scopeName, String schemeName, String fieldName, Long initialValue, Long stepping);
	
	Boolean flushScope(String scopeName, String schemeName, String fieldName, Long initialValue, Long stepping);
	
	void removeScope(String scopeName, String schemeName, String fieldName);
	
	void flushBuffer(String scopeName, String schemeName, String fieldName);
	
	void setBufferSize(String scopeName, String schemeName, String fieldName, Integer size);
	
	Long nowId(String scopeName, String schemeName, String fieldName);
	
	Long next(String scopeName, String schemeName, String fieldName);
	
	Boolean ifExistScope(String scopeName, String schemeName, String fieldName);
}
