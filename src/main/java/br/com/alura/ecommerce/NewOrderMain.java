package br.com.alura.ecommerce;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties());
		
		for(int i=0; i<100; i++) {
			String key = UUID.randomUUID().toString();
			String value = key + ",645645,1234";
			ProducerRecord<String, String> pedidoRecord = new ProducerRecord<String, String>("LOJA_NOVO_PEDIDO", key, value);
			
			Callback callback = (data, ex) ->{
				if( ex != null) {
					ex.printStackTrace();
					return;
				}
				System.out.println("Enviado " + data.topic() + 
						":::partition " + data.partition() + 
						"/ offset " + data.offset() + 
						"/ timeStamp " + data.timestamp());
			};
			
			producer.send(pedidoRecord, callback).get();
			
			String email = "Bem vindo, estamos processando seu pedido";
			ProducerRecord<String, String> emailRecord = new ProducerRecord<String, String>("LOJA_SEND_EMAIL", key, email);
			
			producer.send(emailRecord).get();
			
		}
		
		producer.close();
	}

	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
				"127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
				StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
				StringSerializer.class.getName());
		return properties;
	}

}
