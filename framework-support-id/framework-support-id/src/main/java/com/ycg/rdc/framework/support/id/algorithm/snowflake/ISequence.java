package com.ycg.rdc.framework.support.id.algorithm.snowflake;

public interface ISequence {
	long nextId();
	
	long nowId();
}
