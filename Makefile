network:
	@echo "Checking if Docker network 'cvh-backend-network' exists..."
	@if ! docker network inspect cvh-backend-network >/dev/null 2>&1; then \
		echo "Creating Docker network 'cvh-backend-network'..."; \
		docker network create cvh-backend-network; \
	else \
		echo "Network cvh-backend-network already exists."; \
	fi

mongodb:
	make network
	@echo "Starting the MongoDB container..."
	docker run -d --name mongodb --network cvh-backend-network --network-alias cvh-backend -p 27017:27017 mongo:latest
	@echo "Restoring database..."
	docker run --rm --name mongodb-restore --network cvh-backend-network --volume $(shell pwd)/database:/database mongo:latest mongorestore --gzip --host cvh-backend:27017 /database
	@echo "MongoDB container is up and running on port 27017."

api:
	make network
	@echo "Building the API Docker image..."
	docker build -t api -f Dockerfile.api .
	@echo "Starting the API container..."
	docker run -d --name api --network cvh-backend-network --network-alias cvh-backend -p 8000:8000 -e SYNC_API_KEY=$(SYNC_API_KEY) -e SYNC_DATA_DIR=$(SYNC_DATA_DIR) api
	@echo "API container is up and running on port 8000 (http://0.0.0.0:8000/metadata)."
