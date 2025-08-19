Для запуска необходимо настроить конфигурацию
1) Name SingleMessageConsumer, основной класс com.example.kafka.KafkaApplication, Active profiles SingleMessageConsumer;
2) Name BatchMessageConsumer, основной класс com.example.kafka.KafkaApplication, Active profiles BatchMessageConsumer;
3) Name Producer, основной класс com.example.kafka.KafkaApplication, Active profiles Producer 

Далее поднимаем сервисы, запускаем конфигурации Single..., потом Batch и потом Producer. Хотя порядок не особо и важен.

После отработки продюсера в консоли SingleMessageConsumer увидим пришедшее сообщение. 
Отправим сообщение 10 раз -> увидим в консоли у BatchMessageConsumer всю пачку из 10.
   
topic.txt находится в main\resources
