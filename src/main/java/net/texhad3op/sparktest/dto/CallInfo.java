package net.texhad3op.sparktest.dto;

import java.io.Serializable;

public class CallInfo implements Serializable {
	private String caller;
	private String phone;
	private String start;
	private String finish;
	
	public CallInfo(String caller, String phone, String start, String finish) {
		this.caller = caller;
		this.phone = phone;
		this.start = start;
		this.finish = finish;
	}

	public String getCaller() {
		return caller;
	}

	public void setCaller(String caller) {
		this.caller = caller;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getFinish() {
		return finish;
	}

	public void setFinish(String finish) {
		this.finish = finish;
	}

	@Override
	public String toString() {
		return "CallInfo [caller=" + caller + ", phone=" + phone + ", start=" + start + ", finish=" + finish + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((caller == null) ? 0 : caller.hashCode());
		result = prime * result + ((finish == null) ? 0 : finish.hashCode());
		result = prime * result + ((phone == null) ? 0 : phone.hashCode());
		result = prime * result + ((start == null) ? 0 : start.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CallInfo other = (CallInfo) obj;
		if (caller == null) {
			if (other.caller != null)
				return false;
		} else if (!caller.equals(other.caller))
			return false;
		if (finish == null) {
			if (other.finish != null)
				return false;
		} else if (!finish.equals(other.finish))
			return false;
		if (phone == null) {
			if (other.phone != null)
				return false;
		} else if (!phone.equals(other.phone))
			return false;
		if (start == null) {
			if (other.start != null)
				return false;
		} else if (!start.equals(other.start))
			return false;
		return true;
	}

}
