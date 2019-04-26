package com.gjxx.spark_learn

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class Producer {

  def start(): Unit = {
    println("开始生产消息......")
    val props = new Properties()
    props.put("metadata.broker.list", "cdh1:9092,cdh2:9092,cdh3:9092")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh1:9092,cdh2:9092,cdh3:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      for (i <- 0 to 10) {
        producer.send(new ProducerRecord("test", "key-" + i, "msg-" + i))
        Thread.sleep(3000)
      }
    }

    producer.close()
  }

}

object Producer {
  def main(args: Array[String]): Unit = {
    new Producer().start()
  }
}
