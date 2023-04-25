
from fastapi import FastAPI

app = FastAPI()

@app.post("/scrape/")
async def scrape_website(url: str):
    # Your code to scrape website goes here
    return {"message": "Website scraped successfully!"}
