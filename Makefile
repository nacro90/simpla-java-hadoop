run:
	docker-compose up -d && \
		mvn package -DskipTests && \
		mvn exec:java -Dexec.mainClass="org.orcan.App"

clean:
	mvn clean install -DskipTests


