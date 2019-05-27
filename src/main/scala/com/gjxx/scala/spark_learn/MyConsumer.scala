package com.gjxx.scala.spark_learn

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

class MyConsumer {

  val BOOTSTRAP_SERVER = "cdh1:9092,cdh2:9092,cdh3:9092"
  val GROUP_ID = "sxs"
  val TOPIC = "test"

  def start(): Unit = {
    val consumer = createConsumer()
    println(consumer)
    while (true) {
      val records = consumer.poll(4000)
      val iter = records.iterator()
      while (iter.hasNext) {
        println("receive: " + iter.next())
      }
    }

  }

  def createConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", BOOTSTRAP_SERVER)
    // 每个消费者分配独立的组号
    props.put("group.id", GROUP_ID)
    // 如果value合法，则自动提交偏移量
    props.put("enable.auto.commit", "true")
    // 设置多久一次更新被消费消息的偏移量
    props.put("auto.commit.interval.ms", "1000")
    // 设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(TOPIC))
    println("Subscribed to topic " + TOPIC)
    consumer
  }

}

object MyConsumer {
  def main(args: Array[String]): Unit = {
    new MyConsumer().start()
  }
}
