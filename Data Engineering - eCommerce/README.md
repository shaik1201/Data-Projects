# Data Engineering Project: Ecommerce Data Simulation and Processing Pipeline

This project implements a real-time e-commerce data processing pipeline using modern data engineering tools and technologies. It simulates e-commerce data, processes it through various stages, and stores it in a relational database, all containerized using Docker.

## Project Architecture

The system architecture includes the following components:

- **Data Simulator**: Simulates ecommerce data and sends it to Kafka (using Flask).
- **Kafka**: Streams the simulated data.
- **Spark**: Processes the streamed data.
- **MySQL**: Stores the processed data.

![System Architecture](https://github.com/shaik1201/Data-Projects/blob/main/Data%20Engineering%20-%20eCommerce/Architecture.png)

## Requirements

To run this project locally, you need to have the following installed:

- Docker
- Docker Compose

## Getting Started

Follow these steps to get the project up and running on your local machine.

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
```

### Step 2: Build and Start the Docker Containers

Run the following command to build and start all containers using Docker Compose:
```bash
docker-compose up --build -d
```
This command will start the following services:

Data Simulator: Generates simulated ecommerce data and streams it to Kafka. <br>
Kafka: Kafka instance for streaming the data.<br>
Spark: Spark job that consumes the Kafka stream, processes it, and stores the results in MySQL.<br>
MySQL: MySQL database where processed data is stored.<br>
The services will run in the background, and you will be able to track logs to monitor the process.

