/**
 * 
 */
package edu.nyu.adbms;

import java.util.Date;

/**
 * @author Pratyush Anand (pa1139)
 * @author Arpit Saini (ads745)
 *
 */
public class Transaction {

	public static enum Type { RO, RW };
	private int _transId;
	private Date _timestamp;
	private Type _type;
	
	/**
	 * @param _transId
	 * @param _timestamp
	 * @param _type
	 */
	public Transaction(int _transId, Date _timestamp, Type _type) {
		super();
		this._transId = _transId;
		this._timestamp = _timestamp;
		this._type = _type;
	}

	/**
	 * @return the _transId
	 */
	public int get_transId() {
		return _transId;
	}

	/**
	 * @param _transId the _transId to set
	 */
	public void set_transId(int _transId) {
		this._transId = _transId;
	}

	/**
	 * @return the _timestamp
	 */
	public Date get_timestamp() {
		return _timestamp;
	}

	/**
	 * @param _timestamp the _timestamp to set
	 */
	public void set_timestamp(Date _timestamp) {
		this._timestamp = _timestamp;
	}

	/**
	 * @return the _type
	 */
	public Type get_type() {
		return _type;
	}

	/**
	 * @param _type the _type to set
	 */
	public void set_type(Type _type) {
		this._type = _type;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _transId;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Transaction other = (Transaction) obj;
		if (_transId != other._transId)
			return false;
		return true;
	}
}
