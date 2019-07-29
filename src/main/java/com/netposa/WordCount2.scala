package com.netposa

import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("WC2").setMaster("local[*]");
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf);
    //使用sc创建RDD并执行相应的transformation和action
    val file=sc.textFile("src/main/resources/hello.txt");

    println(file.collect+"___________-")


    file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile("imyspark");
    //停止sc，结束该任务
    sc.stop();


  }
}
