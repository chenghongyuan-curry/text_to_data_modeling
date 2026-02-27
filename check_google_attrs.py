import os
import google.genai
from google import genai
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

print("Checking model attributes...")
for model in client.models.list():
    print(f"Model: {model.name}")
    print(f"Dir: {dir(model)}")
    break
