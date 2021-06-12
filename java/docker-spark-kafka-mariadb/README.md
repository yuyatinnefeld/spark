# Spark Structured Streaming + Kafka

## Streaming Patterns
- Streaming Analytics Pattern: 
  
Realtime data streaming Analyze and publish insights for consumption by real-time dashboard and actions
- Alerts and thresholds Pattern: 
  
Realtime data streaming Analyze to compare against thresholds, then publish exceptions to special output streams
- Leaderboards Pattern: 
  
Realtime data streaming Analyze and update and maintain a leaderboard that shows the top X elements
- Real-Time Prediction Pattern: 
  
Realtime data streaming Analyze to predict outcomes and behavior and publish them to outgoing streams

## Streaming Analytics Pattern
![GitHub Logo](/images/analytics.png)

### Use Cases Design
![GitHub Logo](/images/analytics2.png)

### Use Cases
- Ecommerce: Orders and activities Analysis
- IT operation: System and service loads
- UX: Sentiment & Engagement analysis

#### step1 - kafka setup
> READ resources/kafka.txt

#### step2 - mariaDB setup
> READ resources/MariaDB.txt

#### step3 - redis setup
> READ resources/Redis.txt

#### step4 - run kafka & spark apps
1. chapter2.KafkaOrdersDataGenerator
2. chapter2.StreamingAnalytics

## Alerts and Thresholds Pattern
![GitHub Logo](/images/alerting.png)

### Use Cases Design
![GitHub Logo](/images/alerting2.png)

### Use Cases
- ECommerce: Aborted shopping carts by product to check the reason and to provide customized marketing events
- IT operation: Failures above thresholds
- Media: Negative reactions by topic
- UX: session aborts by user properties (demography, behavior)

#### step1 - kafka consumer run

```bash
> kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.alerts.input
> kafka-kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.alerts.highvolume
> kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.alerts.critical
```
#### step2 - kafka & spark app run
1. chapter2.KafkaAlertsDataGenerator
2. chapter3.StreamingAlerts


## Leaderboards Pattern
![GitHub Logo](/images/leaderboard.png)

### Use Cases
- ECommerce: Top trending product items, groups
- IT operation: Top exception codes
- Media: Top trending topics
- UX: Top engagements elements

#### step1 - kafka consumer run

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.leaderboards.input
```

#### step2 - kafka & spark app run
1. chapter4.KafkaReviewsDataGenerator
2. chapter4.StreamingLeaderboard

## Real-Time Predictions Pattern
![GitHub Logo](/images/predictions.png)

### Use Case Design
![GitHub Logo](/images/predictions2.png)

### Use Cases
- Ecommerce: Product recommendations
- IT operations: System and service failures
- Media: Recommendations
- UX: Engagement

#### step1 - kafka consumer run
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.sentiment.output --property print.key=true
```

#### step2 - kafka & spark app run
1. chapter5.KafkaReviewsDataGenerator
2. chapter5.RunLocalWebServer
3. chapter5.StreamingPredictions

## Real WebAnalytics 
Pattern => Leaderboard Pattern

### Use Case Design
![GitHub Logo](/images/webviews.png)

#### step1 - kafka consumer run
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.views.input
```

#### step2 - kafka & spark app run
1. chapter6.KafkaViewsDataGenerator
2. chapter6.WebsiteViewsAnalytics
