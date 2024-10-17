# Kafka Toolkit
This toolkit is a collection of scripts and tools to help you manage your Kafka cluster.

# How to use

Install the package with the following command:
```bash
go install github.com/suryatresna/kafka-toolkit@latest
```

After that, you can use the following command to see the available commands:
```bash
kafka-toolkit produce --brokers 127.0.0.1:19092 --topic FooTopic  --jsonfile data/sample1.json
```

# Feature Incoming
- [ ] Client consuming
- [ ] Create Topic
- [ ] Delete Topic
