import os
import json
import time
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import boto3
from dotenv import load_dotenv

# Force load the .env file from the current working directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

app = FastAPI(title="CloudScale E-Commerce Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')

# Print startup diagnostics to confirm variables loaded
print("--- AWS CONFIG CHECK ---")
print(f"Access Key Loaded: {'YES' if AWS_ACCESS_KEY else 'NO (Missing)'}")
print(f"Secret Key Loaded: {'YES' if AWS_SECRET_KEY else 'NO (Missing)'}")
print(f"Region: {AWS_REGION}")
print(f"S3 Bucket: {S3_BUCKET}")
print("------------------------")

class CheckoutRequest(BaseModel):
    user_id: int
    item: str
    amount: float

@app.get("/")
def read_root():
    return {"status": "online", "message": "Python FastAPI Backend is Running!"}

@app.post("/api/checkout")
async def process_checkout(payload: CheckoutRequest):
    try:
        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
            raise ValueError("AWS credentials are missing in .env file!")
        if not S3_BUCKET:
            raise ValueError("S3_BUCKET_NAME is missing in .env file!")

        # Initialize S3 client inside endpoint call
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        order_id = int(time.time() * 1000) % 1000000
        event_time = datetime.now(timezone.utc).isoformat()

        event_payload = [{
            "event_id": f"evt_{order_id}",
            "user_id": payload.user_id,
            "action": "checkout",
            "item": payload.item,
            "amount": payload.amount,
            "timestamp": event_time
        }]

        file_key = f"api-events/{int(time.time())}_evt_{order_id}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=file_key,
            Body=json.dumps(event_payload),
            ContentType="application/json"
        )

        print(f"✅ Success: Order #{order_id} pushed to s3://{S3_BUCKET}/{file_key}")

        return {
            "status": "success",
            "order_id": order_id,
            "s3_path": f"s3://{S3_BUCKET}/{file_key}"
        }

    except Exception as e:
        print(f"❌ Error during checkout: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
