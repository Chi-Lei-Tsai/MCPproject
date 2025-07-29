# azure_test.py
import os
from typing import cast, List
from dotenv import load_dotenv
from openai import AzureOpenAI
from openai.types.chat import ChatCompletionMessageParam  # <-- for type hint

load_dotenv()

client = AzureOpenAI(
    api_key        = os.environ["AOAI_KEY"],               # str, never None
    azure_endpoint = os.environ["AOAI_URL"],               # str, never None
    api_version    = os.getenv("OPENAI_API_VERSION") or "2023-05-15",
)

DEPLOYMENT = "pgdemo-gpt4"

def main() -> None:
    prompt = input("❯ Ask Azure GPT: ")

    messages: List[ChatCompletionMessageParam] = [         # type correct
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user",   "content": prompt},
    ]

    resp = client.chat.completions.create(
        model = DEPLOYMENT,
        temperature   = 0.7,
        messages      = messages,
    )

    print("Assistant →", resp.choices[0].message.content.strip())

if __name__ == "__main__":
    main()
