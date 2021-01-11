# aws-cloudwatch-log4j2
AWS CloudWatch Log4j2 Appender

### log4j2.xml file example
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN" packages="pro.apphub.aws.cloudwatch.log4j2">
	<Appenders>
		<CloudWatchAppender name="cwLogger" group="my/group" streamPrefix="prefix">
			<PatternLayout>
				<Pattern>[%-5level] %d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} - %msg%n</Pattern>
			</PatternLayout>
		</CloudWatchAppender>
	</Appenders>
	<Loggers>
		<Root level="info" additivity="false">
			<appender-ref ref="cwLogger"/>
		</Root>
	</Loggers>
</Configuration>
```