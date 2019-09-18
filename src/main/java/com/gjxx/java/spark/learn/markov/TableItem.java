package com.gjxx.java.spark.learn.markov;

/**
 * @author Sxs
 */
public class TableItem  {
	String fromState;
	String toState;
	int count;
	
	public TableItem(String fromState, String toState, int count) {
		this.fromState = fromState;
		this.toState = toState;
		this.count = count;
	}
	
	/**
	 * for debugging ONLY
	 */
	@Override
	public String toString() {
		return "{"+fromState+"," +toState+","+count+"}";
	}
}