package com.ra.firstkafka.application.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	private static final String BOOT_SERVERS = "127.0.0.1:9092";

	public static void main(String[] args) {
		/*
		 * Per creare un producer Kafka dobbiamo svolgere i seguenti 3 step:
		 * 	-> 1. Crare un file di properties per il producer 
		 *  -> 2. Creare oggetto Producer
		 *  -> 3. Inviare dei dati (altrimenti che producer del c***o è?) NOTA BENE: --> invio dei dati è asincrono
		 *  -> 4. esguire flush per fare in modo che i dati vengonon inviati
		 *  -> 5. esguire close del producer
		 *  
		 *  NB. Se dimentichiamo di eseguire il flush e close del producer, allora i dati non arriveranno mai al topic relativo (situato nel brokoer)
		 * */
		
		// -> step 1
		// =====================================================================================
		Properties producerProperties = new Properties(); 
		// Domanda: cosa dobbiamo mettere dentro le properties ??? 
		// Risposta: https://kafka.apache.org/documentation/#producerconfigs :)
		producerProperties.setProperty("bootstrap.servers", BOOT_SERVERS);					   // specifica dove si trova il bootstrap server, ovvero il broker 
		producerProperties.setProperty("key.serializer", StringSerializer.class.getName());	   // specifica come serializzare le chiavi una volta arrivati al broker
		producerProperties.setProperty("value.serializer", StringSerializer.class.getName());  // specifica come serializzare i messaggi una volta arrivati al broker
		// aggiungiamo altre property per altre configurazioni del producer se necessario ....
		// ======================================================================================
		
		// -> step 2
		// =======================================================================================
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
		// ========================================================================================
		
		// -> step 3
		// =======================================================================================
		String topic = "first_topic";
		String message = "This is a message to send in topic called first_topic, Bye";
		ProducerRecord<String, String> producerRecordToSend =  new ProducerRecord<String, String>(topic, message);
		producer.send(producerRecordToSend);
		// =======================================================================================
		
		// -> step 4
		// =======================================================================================
		producer.flush();
		// =======================================================================================

		// -> step 5
		// =======================================================================================
		producer.close();
		// =======================================================================================
	}
}
