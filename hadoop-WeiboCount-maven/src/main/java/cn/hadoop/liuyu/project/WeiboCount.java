package cn.hadoop.liuyu.project;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeiboCount extends Configured implements Tool {
	public static class WeiBoMapper extends
			Mapper<Text, WeiBo, Text, Text> {
		@Override
		protected void map(Text key, WeiBo value, Context context)
				throws IOException, InterruptedException {
			context.write(new Text("follower"),
					new Text(key.toString() + "\t" + value.getFollowers()));
			context.write(new Text("friend"),
					new Text(key.toString() + "\t" + value.getFriends()));
			context.write(new Text("statuses"),
					new Text(key.toString() + "\t" + value.getStatuses()));
		}
	}
	public static class WeiBoReducer extends Reducer<Text, Text, Text, IntWritable> {
		private MultipleOutputs<Text, IntWritable> mos;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		private Text text = new Text();

		protected void reduce(Text Key, Iterable<Text> Values,
				Context context) throws IOException, InterruptedException {
			int N = context.getConfiguration().getInt("reduceHasMaxLength", Integer.MAX_VALUE);
			Map< String,Integer> m = new HashMap< String,Integer>();
			for(Text value:Values){
				//value=名称+(粉丝数 或 关注数 或 微博数)
				String[] records = value.toString().split("\t");
				m.put(records[0], Integer.parseInt(records[1].toString()));
			}
			//对Map内的数据进行排序
			Map.Entry< String, Integer>[] entries = getSortedHashtableByValue(m);
			for(int i = 0; i< N&&i< entries.length;i++){
				if(Key.toString().equals("follower")){
					mos.write("follower",entries[i].getKey(), entries[i].getValue());
				}else if(Key.toString().equals("friend")){
					mos.write("friend", entries[i].getKey(), entries[i].getValue());
				}else if(Key.toString().equals("status")){
					mos.write("statuses", entries[i].getKey(), entries[i].getValue()); 
				}
			}               
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}
	

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 配置文件对象
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job =Job.getInstance(conf, "weibo");// 构造任务
		job.setJarByClass(WeiboCount.class);// 主类

		job.setMapperClass(WeiBoMapper.class);// Mapper
		job.setMapOutputKeyClass(Text.class);// Mapper key输出类型
		job.setMapOutputValueClass(Text.class);// Mapper value输出类型
        
		job.setReducerClass(WeiBoReducer.class);// Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
		job.setInputFormatClass(WeiboInputFormat.class);// 自定义输入格式
		//自定义文件输出类别
		MultipleOutputs.addNamedOutput(job, "follower", TextOutputFormat.class,
						Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "friend", TextOutputFormat.class,
				Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "status", TextOutputFormat.class,
				Text.class, IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}
	//对Map内的数据进行排序（只适合小数据量）
	public static Map.Entry[] getSortedHashtableByValue(Map h) {  
		Set set = h.entrySet();  
		Map.Entry[] entries = (Map.Entry[]) set.toArray(new Map.Entry[set.size()]);  
		Arrays.sort(entries, new Comparator() {  
			public int compare(Object arg0, Object arg1) {  
				Long key1 = Long.valueOf(((Map.Entry) arg0).getValue().toString());  
				Long key2 = Long.valueOf(((Map.Entry) arg1).getValue().toString());  
			return key2.compareTo(key1);  
		} });
		return entries;  
	}
	
	public static void main(String[] args) throws Exception {
		int ec = ToolRunner.run(new Configuration(), new WeiboCount(), args);
		System.exit(ec);
	}
}
