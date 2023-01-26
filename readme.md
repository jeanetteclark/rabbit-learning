# rabbit-learning

This repo contains a toy program to implement some features of RabbitMQ found in metadig-engine

I started with the [RabbitMQ tutorials](https://www.rabbitmq.com/tutorials/tutorial-one-java.html) and
then expanded to mirror (very roughly) what RabbitMQ does in the `Worker` class of metadig-engine.

I run all of this standalone, you'll need to download the RabbitMQ [client library](https://repo1.maven.org/maven2/com/rabbitmq/amqp-client/5.7.1/amqp-client-5.7.1.jar), its deps [SLF4J API](https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.26/slf4j-api-1.7.26.jar) and [SLF4J Simple](https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.26/slf4j-simple-1.7.26.jar), and put them in the working directory.

First start RabbitMQ either in the foreground

```
CONF_ENV_FILE="/opt/homebrew/etc/rabbitmq/rabbitmq-env.conf" /opt/homebrew/opt/rabbitmq/sbin/rabbitmq-server
```

or background

```
brew services start rabbitmq
brew services stop rabbitmq
```

Set a variable to put the RMQ library on the classpath

```
export CP=.:amqp-client-5.7.1.jar:slf4j-api-1.7.26.jar:slf4j-simple-1.7.26.jar
```

Compile

```
javac -cp $CP NewTask.java Worker.java Completed.java
```

Then in one terminal start the Worker queue

```
java -cp $CP Worker
```

In another start the Completed queue

```
java -cp $CP Completed
```

Finally, in a third send your task. The more periods included the longer it will take to execute
See the `doWork` method in the Worker class.

```
java -cp $CP NewTask Hello...
```

