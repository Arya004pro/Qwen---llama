import os
import time
import requests


API_BASE = os.getenv("MOTIA_API_URL", "http://host.docker.internal:3121").rstrip("/")
SUBMIT_URL = f"{API_BASE}/query"


def fetch_result(query_id: str) -> dict:
    url = f"{API_BASE}/query/{query_id}"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def print_final(result: dict) -> None:
    status = result.get("status")
    if status == "completed":
        text = result.get("formattedText")
        if text:
            print(f"Assistant: {text}")
        else:
            print("Assistant: Query completed.")
        return

    if status == "needs_clarification":
        msg = result.get("clarification", "Please clarify your query.")
        print(f"Assistant: {msg}")
        return

    if status == "error":
        err = result.get("error", "Unknown error.")
        print(f"Assistant: {err}")
        return

    print(f"Assistant: Query finished with status '{status}'.")


def main() -> None:
    print("Motia Live Chat (type 'exit' to quit)")
    print(f"Using Motia API: {API_BASE}")

    while True:
        user_input = input("\nUser: ").strip()
        if user_input.lower() in {"exit", "quit", "q"}:
            print("Assistant: Goodbye")
            return
        if not user_input:
            continue

        try:
            submit = requests.post(SUBMIT_URL, json={"query": user_input}, timeout=20)
            submit.raise_for_status()
            data = submit.json()
        except Exception as exc:
            print(f"Assistant: Failed to submit query to Motia ({exc}).")
            continue

        query_id = data.get("queryId")
        if not query_id:
            print("Assistant: No queryId returned from Motia.")
            continue

        print(f"Assistant: Accepted (queryId: {query_id}). Processing...")

        deadline = time.time() + 180
        last_status = None
        while time.time() < deadline:
            try:
                result = fetch_result(query_id)
            except requests.HTTPError as exc:
                # Step may not have persisted yet; brief retry.
                if exc.response is not None and exc.response.status_code == 404:
                    time.sleep(0.7)
                    continue
                print(f"Assistant: Failed to fetch result ({exc}).")
                break
            except Exception as exc:
                print(f"Assistant: Failed to fetch result ({exc}).")
                break

            status = result.get("status")
            if status != last_status:
                print(f"[status] {status}")
                last_status = status

            if status in {"completed", "error", "needs_clarification"}:
                print_final(result)
                break

            time.sleep(0.7)
        else:
            print("Assistant: Timed out waiting for result. Check Motia Logs/Traces UI.")


if __name__ == "__main__":
    main()
