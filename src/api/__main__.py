import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "src.api:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),  # nosec B104
        port=int(os.environ.get("API_PORT", "8000")),
    )
