package com.example.kscdatagenerator.generator;

import com.example.kscdatagenerator.entity.Employee;
import com.example.kscdatagenerator.repository.EmployeeRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@Component
public class DataGenerator implements CommandLineRunner {

    @Autowired
    private EmployeeRepository employeeRepository;

    @Override
    public void run(String... args) throws Exception {
        generateData();
    }

    public void generateData() throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Properties adminClientProps = new Properties();
        adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try (AdminClient adminClient = AdminClient.create(adminClientProps)) {

            List<Employee> employees = employeeRepository.findAll();

            while (true) {
                Employee randomEmployee = employees.get(new Random().nextInt(employees.size()));
                String topicName = "user-" + randomEmployee.getEmpno();

                try {
                    if (!adminClient.listTopics().names().get().contains(topicName)) {
                        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                        adminClient.createTopics(Collections.singletonList(newTopic));
                        System.out.println(topicName + " konusu oluşturuldu.");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("Konu listesi alınırken hata oluştu: " + e.getMessage());
                }

                String expenseRecord = generateRandomExpenseRecord(randomEmployee.getEmpno());
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, expenseRecord);
                producer.send(record);

                Thread.sleep(1000);
            }
        }
    }

    private String generateRandomExpenseRecord(Integer userId) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String dateTime = dateFormat.format(new Date());
        String[] descriptions = {"Coffee", "Book", "Meal", "Transport", "Fashion"};
        String description = descriptions[new Random().nextInt(descriptions.length)];
        String[] types = {"Drinks", "Education", "Foods", "Travel", "Shopping"};
        String type = types[new Random().nextInt(types.length)];
        int count = new Random().nextInt(5) + 1;
        double payment = Math.round(new Random().nextDouble() * 100.0 * 100.0) / 100.0;

        return String.format("%d, %s, \"%s\", \"%s\", %d, %.2f", userId, dateTime, description, type, count, payment);
    }
}