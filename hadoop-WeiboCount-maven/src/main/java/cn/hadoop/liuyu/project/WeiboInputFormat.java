package cn.hadoop.liuyu.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class WeiboInputFormat extends FileInputFormat<Text,WeiBo>{

     @Override
     public RecordReader<Text, WeiBo> createRecordReader(InputSplit arg0,
               TaskAttemptContext arg1) throws IOException, InterruptedException {
          //这里默认是系统实现的的RecordReader，按行读取，下面我们自定义这个类WeiboRecordReader。
          return new WeiboRecordReader();
     }

     public class WeiboRecordReader extends RecordReader<Text, WeiBo>{
    		public LineReader in;  
    		public Text lineKey; //声明key类型
    		public WeiBo lineValue;//声明 value类型
    		public Text line;
    		
    		@Override
    		public void initialize(InputSplit input, TaskAttemptContext context)
    				throws IOException, InterruptedException {
    			FileSplit split=(FileSplit)input;//获取split   
    			Configuration job=context.getConfiguration();  
    			Path file=split.getPath();//得到文件路径     
    			FileSystem fs=file.getFileSystem(job); 
    			
    			FSDataInputStream filein=fs.open(file);//打开文件   
    			in=new LineReader(filein,job); 
    			line=new Text();  
    			lineKey=new Text();//新建一个Text实例作为自定义格式输入的key
    			lineValue = new WeiBo();//新建一个WeiBo实例作为自定义格式输入的value
    		}

    		public boolean nextKeyValue() throws IOException, InterruptedException {
    			int linesize=in.readLine(line); 
    			if(linesize==0)  return false; 
    			//通过分隔符'\t'，将每行的数据解析成数组 pieces
    			String[] pieces = line.toString().split("\t"); 
    			if(pieces.length != 5){  
    				throw new IOException("Invalid record received");  
    			} 
    			int a,b,c;
    			try{  
    				a = Integer.parseInt(pieces[2].trim());//粉丝  
    				b = Integer.parseInt(pieces[3].trim());//关注
    				c = Integer.parseInt(pieces[4].trim());//微博数
    			}catch(NumberFormatException nfe){  
    				throw new IOException("Error parsing floating poing value in record");  
    			} 
    			//自定义key和value值
    			lineKey.set(pieces[0]);  
    			lineValue.set(b, a, c);
    			return true;
    		}
    		
    		@Override
    		public void close() throws IOException {
    			if(in !=null){
    				in.close();
    			}
    		}

    		@Override
    		public Text getCurrentKey() throws IOException,
    				InterruptedException {
    			return lineKey;
    		}

    		@Override
    		public WeiBo getCurrentValue() throws IOException, InterruptedException {
    			return lineValue;
    		}

    		@Override
    		public float getProgress() throws IOException, InterruptedException {
    			return 0;
    		}
    		
    	}
}
		
