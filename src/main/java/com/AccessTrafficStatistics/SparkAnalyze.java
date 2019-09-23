package com.AccessTrafficStatistics;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkAnalyze {
	private static String logpath = "./app_log.txt";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//创建Spark配置文件
		SparkConf conf = new SparkConf().setAppName("AccessTrafficStatistics").setMaster("local");
		//创建上下文对象
		JavaSparkContext sc = new JavaSparkContext(conf);
		//读取文件，创建RDD
		JavaRDD<String> JavaRDD = sc.textFile(logpath);
		//将RDD映射为key-value模式,key为机器ID,value为自定义的数据封装类封装了三个属性(时间戳，上行，下行)
		JavaPairRDD<String, AccessLogInfo> accessLogPairRDD = JavaRDD.mapToPair(
			//JavaPairRDD<K2,V2> mapToPair(PairFunction<T,K2,V2> f)
			//此函数会对一个RDD中的每个元素调用f函数，其中原来RDD中的每一个元素都是T类型的，调用f函数后会进行一定的操作把每个元素都转换成一个<K2,V2>类型的对象
			//进行一行一行的读取
			new PairFunction<String, String, AccessLogInfo>() {
				private static final long serialVersionUID = 1L;//序列化ID
				public Tuple2<String, AccessLogInfo> call(String JavaRDD)
				{
					//按行切分
					String[] rdds = JavaRDD.split("\t");
					//获取四个字段
					long timestamp = Long.valueOf(rdds[0]);
	                String deviceID = rdds[1];
	                long upTraffic = Long.valueOf(rdds[2]);
	                long downTraffic = Long.valueOf(rdds[3]);
	                // 将时间戳，上行流量和下行流量封装为自定义的可序列化对象
	                AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp,upTraffic,downTraffic);
	                //返回一个Scala的Tuple2类型
	                return new Tuple2<String, AccessLogInfo>(deviceID,accessLogInfo);
				}
			}
		);
		//根据deviceID进行聚合求出上行和下行的流量，及其最早访问的时间
		JavaPairRDD<String, AccessLogInfo> aggregateLogPairRDD = accessLogPairRDD.reduceByKey(
			//前两个为输入，后一个为输出
			new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>()
			{
				private static final long serialVersionUID = 1L;
				@Override
				public AccessLogInfo call(AccessLogInfo v1, AccessLogInfo v2) throws Exception {
					// TODO Auto-generated method stub
					long timestamp = v1.getTimestamp() < v2.getTimestamp()?v1.getTimestamp():v2.getTimestamp();//选择时间最早的
	                long upTraffic = v1.getUpTraffic()+v2.getUpTraffic();//上下行流量之和
	                long downTraffic=v1.getDownTraffic()+v2.getDownTraffic();
	                //进行聚合之后产生一个AccessLogInfo
	                AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp,upTraffic,downTraffic);
	                return accessLogInfo;
				}
			}
		);
		//将<deviceID-数据封装类>映射为<自定义排序类,deviceID>
		JavaPairRDD<AccessLogSortKey, String> accessLogSortRDD = aggregateLogPairRDD.mapToPair(
			new PairFunction<Tuple2<String,AccessLogInfo>, AccessLogSortKey, String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<AccessLogSortKey, String> call(Tuple2<String, AccessLogInfo> t) throws Exception {
					// TODO Auto-generated method stub
					String s = t._1;
					AccessLogInfo a = t._2;
					AccessLogSortKey outa = new AccessLogSortKey(a.getTimestamp(), a.getUpTraffic(), a.getDownTraffic());
					return new Tuple2<AccessLogSortKey, String>(outa, s);
				}
			}
		);
		//利用自定义排序类实现的方法进行降序排序,自定义排序类实现了Scala语言中的Ordered接口的compare方法
		JavaPairRDD<AccessLogSortKey, String> sortedAccessLogRDD= accessLogSortRDD.sortByKey(false);
		//获取前 top 10
        List<Tuple2<AccessLogSortKey, String>> top10DataList = sortedAccessLogRDD.take(10);
		for(Tuple2<AccessLogSortKey, String> t: top10DataList)
		{
			System.out.print(t._2 + "\t");
			System.out.print(t._1.getTimestamp() + "\t");
			System.out.print(t._1.getUpTraffic() + "\t");
			System.out.print(t._1.getDownTraffic());
			System.out.println();
		}
        //关闭上下文
        sc.close();
	}

}
