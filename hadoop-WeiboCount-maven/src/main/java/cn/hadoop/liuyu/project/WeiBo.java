package cn.hadoop.liuyu.project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WeiBo implements WritableComparable < Object > {
	////直接利用java的基本数据类型int，定义成员变量friends、followers、statuses
	private int friends;
	private int followers;
	private int statuses;
	
	public int getFriends() {
		return friends;
	}
	
	public void setFriends(int friends) {
		this.friends = friends;
	}
	
	public int getFollowers() {
		return followers;
	}
	
	public void setFollowers(int followers) {
		this.followers = followers;
	}
	
	public int getStatuses() {
		return statuses;
	}
	
	public void setStatuses(int statuses) {
		this.statuses = statuses;
	}
	
	public WeiBo(){};
	
	public WeiBo(int friends,int followers,int statuses){
		this.followers = followers;
		this.friends = friends;
		this.statuses = statuses;
	}
	
	public void set(int friends,int followers,int statuses){
		this.followers = followers;
		this.friends = friends;
		this.statuses = statuses;
	}
	
	// 实现WritableComparable的readFields()方法，以便该数据能被序列化后完成网络传输或文件输入
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		friends  = in.readInt();
		followers = in.readInt();
		statuses = in.readInt();
	}
	
	// 实现WritableComparable的write()方法，以便该数据能被序列化后完成网络传输或文件输出 
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(followers);
		out.writeInt(friends);
		out.writeInt(statuses);
	}
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
}

