package com;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class FindCommonFriends {
    public static class FindCommonFriendsStep1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();     //获取Mapper端的输入值
            String[] userAndfriends = line.split(", ");
            String user = userAndfriends[0];        //获取输入值中的用户名称
            String[] friends = userAndfriends[1].split(" ");        //获取输入值中的好友列表
            for (String friend : friends) {
                context.write(new Text(friend), new Text(user));        //以<B,A><C,A><D,A><F,A>...的格式输出
            }
        }
    }

    public static class FindCommonFriendsStep1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text friend, Iterable<Text> users, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text user : users) {
                sb.append(user).append(",");        //对键值相同的键值对<A,B><A,D><A,F>...进行拼接
            }
            context.write(friend, new Text(sb.toString()));     //以<A   B,D,F, ...>的形式写入到HDFS上，作为中间结果，注意中间是以制表符（\t）分隔开
        }
    }

    public static class FindCommonFriendsStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] friendAndusers = line.split("\t");
            String friend = friendAndusers[0];
            String[] users = friendAndusers[1].split(",");
            Arrays.sort(users);    // 下面的循环实现了对分隔符之后的任意两个用户的拼接，并作为新的key值生成键值对
            for (int i = 0; i < users.length - 1; i++) {
                for (int j = i + 1; j < users.length; j++) {
                    context.write(new Text("[" + users[i] + "," + users[j] + "]"), new Text(friend));
                }
            }
        }
    }

    public static class FindCommonFriendsStep2Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text user, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text friend : friends) {
                sb.append(friend).append(",");    //对key值相同的键值对进行拼接
            }
            String sb1 = sb.toString();
            sb1 = sb1.substring(0,sb1.length() - 1);
            context.write(new Text("(" + user + ","), new Text("[" + sb1 + "])"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();    //获取参数
        if (otherArgs.length < 2) {
            System.err.println("error");
            System.exit(2);    //如果获取到的参数少于两个，报错之后退出作业
        }

        Job job1 = Job.getInstance(conf, "find common friends step1");
        job1.setJarByClass(FindCommonFriends.class);    //加载处理主类
        job1.setMapperClass(FindCommonFriendsStep1Mapper.class);    //指定Mapper类
        job1.setReducerClass(FindCommonFriendsStep1Reducer.class);    //指定Reducer类
        job1.setOutputKeyClass(Text.class);    //指定Reducer输出键的类型
        job1.setOutputValueClass(Text.class);    //指定Reducer输出值的类型

        Path tempDir = new Path("commonfriend-temp-output");
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, tempDir);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "find common friends step2");
        job2.setMapperClass(FindCommonFriendsStep2Mapper.class);
        job2.setReducerClass(FindCommonFriendsStep2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, tempDir);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        job2.waitForCompletion(true);

        FileSystem.get(conf).delete(tempDir);
        System.exit(0);
    }

}
