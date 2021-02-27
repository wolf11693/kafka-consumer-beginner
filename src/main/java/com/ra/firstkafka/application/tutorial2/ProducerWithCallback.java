package com.ra.firstkafka.application.tutorial2;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallback {
	private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class);

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
	
		for(int i=0; i<5; i++) {
			String topic = "first_topic";
			String message = "This is a message" + i + " to send in topic called first_topic, Bye";
			ProducerRecord<String, String> producerRecordToSend =  new ProducerRecord<String, String>(topic, message);
			
			// funzione di callback eseguita da kafka ogni volta che viene inviato un record (messaggio) 
			// oppure ogni volta che si verfica una eccezione
			Callback kafkaCallBack = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception != null) {
						// il record non è stato inviato correttamente :(
						log.error("Errore nell'invio del record in topic={}", metadata.topic());
						exception.printStackTrace();
					} else {
						log.info("Ricevuto nuovo messaggio in topic={}", metadata.topic());
						log.info("Ricevuto nella partizione", metadata.partition());
						log.info("Memorizzato in posizione={} di topic={}", metadata.offset(), metadata.topic());
						log.info("Timestamp di ricezione={}", metadata.timestamp());
					}
				}
			}; 
			
			producer.send(producerRecordToSend, kafkaCallBack);
	
		}
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
