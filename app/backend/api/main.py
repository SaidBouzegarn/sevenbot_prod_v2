from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.backend.api.news_scraper_service import app as news_scraper_router

app = FastAPI(title="SevenBots Backend API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include routers
app.mount("/news-scraper", news_scraper_router)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
