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
public class Data {

	private int _value;
	private int _index;
	private boolean _accessibleForRead;
	private int _lastCommitValue;
	private Date _commitTime;
	
	private int unavailableTime;

	/**
	 * @param _value
	 * @param _index
	 * @param _accessible
	 * @param _commitTime
	 * @param unavailableTime
	 */
	public Data(int _value, int _index) {
		this._value = _value;
		this._index = _index;
		this._accessibleForRead = true;
		this._commitTime = new Date();
		this.unavailableTime = -1;
	}

	/**
	 * @return the _value
	 */
	public int get_value() {
		return _value;
	}

	/**
	 * @param _value the _value to set
	 */
	public void set_value(int _value) {
		this._value = _value;
	}

	/**
	 * @return the _index
	 */
	public int get_index() {
		return _index;
	}

	/**
	 * @param _index the _index to set
	 */
	public void set_index(int _index) {
		this._index = _index;
	}

	/**
	 * @return the _accessible
	 */
	public boolean is_accessible() {
		return _accessibleForRead;
	}

	/**
	 * @param _accessible the _accessible to set
	 */
	public void set_accessibleForRead(boolean _accessible) {
		this._accessibleForRead = _accessible;
	}

	/**
	 * @return the _commitTime
	 */
	public Date get_commitTime() {
		return _commitTime;
	}

	/**
	 * @param _commitTime the _commitTime to set
	 */
	public void set_commitTime(Date _commitTime) {
		this._commitTime = _commitTime;
	}

	/**
	 * @return the unavailableTime
	 */
	public int getUnavailableTime() {
		return unavailableTime;
	}

	/**
	 * @param unavailableTime the unavailableTime to set
	 */
	public void setUnavailableTime(int unavailableTime) {
		this.unavailableTime = unavailableTime;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _index;
		result = prime * result + _value;
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
		if (!(obj instanceof Data))
			return false;
		Data other = (Data) obj;
		if (_index != other._index)
			return false;
		if (_value != other._value)
			return false;
		return true;
	}

	/**
	 * @return the _lastCommitValue
	 */
	public int get_lastCommitValue() {
		return _lastCommitValue;
	}

	/**
	 * @param _lastCommitValue the _lastCommitValue to set
	 */
	public void set_lastCommitValue(int _lastCommitValue) {
		this._lastCommitValue = _lastCommitValue;
	}
}
