# Running the Main File

## Step 1: Set Up Docker Environment
Ensure Docker and Docker Compose are installed on your system. Use the provided Docker Compose file (`docker-compose.yml`) to set up the Kafka environment locally.

```bash
docker-compose up -d
```

## Step 2: Install Dependencies Create a virtual environment (optional but recommended) and install the required Python dependencies using requirements.txt.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
