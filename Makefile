mongodb:
	@echo "Starting the MongoDB container..."
	docker run -d --name mongodb --network cvh-backend-network --network-alias cvh-backend -p 27017:27017 mongo:latest
	@echo "Restoring database..."
	docker run --rm --name mongodb-restore --network cvh-backend-network --volume $(shell pwd)/database:/database mongodb mongorestore --gzip --host cvh-backend:27017 /database
	@echo "MongoDB container is up and running on port 27017."

api:
	@echo "Building the API Docker image..."
	docker build -t api -f Dockerfile.api .
	@echo "Starting the API container..."
	docker run -d --name api --network cvh-backend-network --network-alias cvh-backend -p 8000:8000 api
	@echo "API container is up and running on port 8000 (http://0.0.0.0:8000/metadata)."
