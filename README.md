# Asynchronous AI Inference System

An end-to-end, event-driven machine learning pipeline that demonstrates distributed asynchronous inference. This project integrates Apache Airflow for data orchestration, AWS SQS for message queuing, and Kubernetes for scalable containerized deployment.

## 🏗️ Architecture Overview

The system is decoupled into three primary components:

1. **Model Training (Airflow):** A DAG trains a Logistic Regression model on the Breast Cancer dataset, serializes it via `joblib`, and uploads it to an AWS S3 bucket.
2. **Queue Population (Airflow):** A second DAG reads the test dataset and pushes individual inference requests as JSON messages into an AWS SQS queue.
3. **Inference Consumer (Kubernetes/Docker):** A containerized Python application that continuously polls the SQS queue, downloads the model from S3, generates predictions, writes the results back to S3, and deletes the processed messages.

## 🛠️ Technologies Used

* **Orchestration:** Apache Airflow
* **Cloud Infrastructure:** Amazon Web Services (AWS S3, SQS)
* **Containerization & Orchestration:** Docker, Kubernetes (Minikube)
* **Machine Learning:** Python, Scikit-Learn, Pandas, Joblib
* **Cloud SDK:** Boto3

## 📁 Repository Structure

```text
Asynchronous_AI_inference_system/
│
├── airflow_home/
│
├── dags/
│   ├── .airflowignore
│   ├── ml_pipeline_dag.py
│   └── msg_sqs_dag.py
│
├── data/
│   ├── breast_cancer.csv
│   └── test_features.csv
│
├── models/
│
├── scripts/
│   ├── generate_data.py
│   └── train_model.py
│
├── src/
│   ├── app/
│   │   ├── __init__.py
│   │   └── consumer.py
│   │
│   └── ml_pipeline/
│       ├── __init__.py
│       ├── data.py
│       ├── model.py
│       └── sqs_publisher.py
│
├── .gitignore
├── consumer-deployment.yaml
├── Dockerfile
├── LICENSE
├── README.md
├── requirements.txt
└── setup_airflow.sh         
```
## 🚀 Setup and Execution

### 1. Prerequisites

- Python 3.12
- Docker installed and running
- `Minikube` and `kubectl` installed
- Active AWS credentials with access to S3 and SQS

---

### 2. AWS Configuration

Ensure you have created the following resources in your AWS account:

- An S3 Bucket named `async-ai-inf`
- An SQS Queue named `ml_pipeline_queue`

---

### 3. Orchestration (Apache Airflow)

1. Start your Airflow webserver and scheduler.
2. Trigger the **Model Training DAG** to generate and upload `model.pkl` to your S3 bucket.
3. Trigger the **Queue Population DAG** to load the testing data into your SQS queue.

---

### 4. Deploying the Consumer to Kubernetes

#### Start your local Minikube cluster

```bash
minikube start --driver=docker
```

#### Build the Docker image and load it into Minikube's registry

```bash
docker build -t ai-consumer-app:latest .
minikube image load ai-consumer-app:latest
```

#### Update the deployment configuration

Update the `consumer-deployment.yaml` file with:

- Your AWS credentials
- Your specific S3 model path

Then apply the deployment:

```bash
kubectl apply -f consumer-deployment.yaml
```

#### Monitor the consumer logs

```bash
kubectl logs -l app=ai-consumer -f
```

---

## 📈 Scaling the System

This architecture is designed to handle massive spikes in inference requests.

To scale the system horizontally, increase the number of consumer replicas in Kubernetes:

```bash
kubectl scale deployment ai-consumer-deployment --replicas=3
```

#### Verify the pods are running concurrently

```bash
kubectl get pods
```

---

## 📄 Output Format

For every message processed, the consumer writes a unique JSON file back to the `predictions/` folder in the S3 bucket.

Example output (`predictions/sample_001.json`):

```json
{
  "record_id": "sample_001",
  "prediction": 1,
  "timestamp": "2026-05-07T08:57:08Z"
}
```
