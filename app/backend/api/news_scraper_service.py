from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
from app.backend.scrape.news_scrapper import NewsScrapper
import logging

logger = logging.getLogger(__name__)

class ScraperConfig(BaseModel):
    website_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    login_url: Optional[str] = None
    username_selector: Optional[str] = None
    password_selector: Optional[str] = None
    submit_button_selector: Optional[str] = None
    crawl: bool = True
    max_pages: int = 25

class ScraperResponse(BaseModel):
    articles: List[dict]
    status: str

app = FastAPI()

@app.post("/scrape", response_model=ScraperResponse)
async def scrape_website(config: ScraperConfig, background_tasks: BackgroundTasks):
    try:
        # Convert Pydantic model to dictionary for NewsScrapper
        scraper_config = config.model_dump()
        
        # Initialize scraper
        scraper = NewsScrapper(**scraper_config)
        
        # Perform scraping
        articles = scraper.scrape()
        
        # Process articles into a more serializable format
        processed_articles = []
        for url, article in articles:
            article_dict = {
                "url": url,
                "classification": article.classification if hasattr(article, 'classification') else None,
                "title": article.title[:200] if hasattr(article, 'title') else None,
                "body": article.body[:1000] if hasattr(article, 'body') else None  # Limit body length
            }
            processed_articles.append(article_dict)
        
        # Close the scraper in the background to free up resources
        background_tasks.add_task(scraper.close)
        
        return {
            "articles": processed_articles,
            "status": "success"
        }
    
    except Exception as e:
        logger.error(f"Scraping error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
