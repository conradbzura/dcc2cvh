mongo:
	@echo "Pulling the latest MongoDB Docker image..."
	docker pull mongo:latest
	@echo "Starting the MongoDB container..."
	docker run --name mongodb -d -p 27017:27017 mongo:latest
	@echo "MongoDB container is up and running on port 27017."
