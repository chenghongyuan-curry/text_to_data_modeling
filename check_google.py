import os
import google.genai
from google import genai
from dotenv import load_dotenv

load_dotenv()

api_key = os.environ.get("GEMINI_API_KEY")
print(f"API Key present: {bool(api_key)}")

try:
    client = genai.Client(api_key=api_key)
    print("Client initialized.")
    
    print("Listing models...")
    models = client.models.list()
    print(f"Models object type: {type(models)}")
    
    count = 0
    for model in models:
        print(f"Model: {model.name}")
        count += 1
        if count >= 5: break
    
except Exception as e:
    print(f"Error: {e}")
