run:
	docker-compose up -d && \
		mvn clean install -DskipTests && \
		mvn exec:java -Dexec.mainClass="org.orcan.App"


