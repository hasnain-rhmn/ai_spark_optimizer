import os

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

API_BASE_URL = "http://localhost:5436/v1"
ORGANIZATION_ID = "31bd6b32-bb73-48d5-80dd-b6b91f6ce1b1"
DEFAULT_HEADERS = {
    "Rpc-Service": "genai-api",
    "Rpc-Caller": "spt",
}
DEFAULT_MODEL = "spt-genai/openai-gpt5-5"

llm = ChatOpenAI(
    model=DEFAULT_MODEL,
    base_url=API_BASE_URL,
    api_key=os.getenv("OPENAI_API_KEY", "placeholder"),
    organization=ORGANIZATION_ID,
    default_headers=DEFAULT_HEADERS,
    temperature=0,
)
