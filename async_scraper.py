import asyncio
import httpx
from typing import List, Optional, Dict
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

class Scraper: 
    """
        Scraper class to fetch data from a given url using async httpx client

        To run, use asyncio.run(Scraper.run(urls))
        
        Parameters:
            concurrent_requests (int): Number of concurrent requests to make. Default is 10.
            

    """

    def __init__(self, concurrent_requests: int = 10):

        self.retrieve_date = datetime.now()

        self.semaphore = asyncio.Semaphore(concurrent_requests)
        self.client = httpx.AsyncClient()
        self.successful_fetches = 0
    async def __aenter__(self) -> "Scraper":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.client.aclose()

    
    @staticmethod
    async def fetch_json(self, url: str) -> Optional[Dict]:
        """
        Fetch JSON data from a single URL, using a semaphore to control concurrency.
        
        Args:
            url (str): The URL to fetch data from.
            semaphore: Limits concurrent fetches.
            
        Returns:
            dict: JSON object containing the data fetched from the URL.
        """

        async with self.semaphore:
            try:
                response = await self.client.get(url)
                response.raise_for_status()  # Raise an exception for 4xx/5xx status codes
                await asyncio.sleep(1)
                logging.info(f"Fetched data from {url}")
                self.successful_fetches += 1
                return response.json()
            except httpx.HTTPStatusError as e:
                logging.error(f'HTTP error fetching data from {url}: {e}')
            except httpx.RequestError as e:
                logging.error(f'Request error fetching data from {url}: {e}')
                try: # Retrying once
                    response = await self.client.get(url)
                except Exception as e:
                    return None
            except Exception as e:
                logging.error(f'Unexpected error fetching data from {url}: {e}')
            return None

    async def fetch_data(self, urls_list: List[str]) -> List[Optional[Dict]]:
        """
        Fetch data from the a list of urls and return a list of JSON objects.
        
        Args:
            urls_list (list): List of URLs to fetch data from.
        
        Returns:
            list: List of JSON objects containing the data fetched from the URLs.
        """
        tasks = [self.fetch_json(self, url) for url in urls_list]
        results = await asyncio.gather(*tasks)
        print(f"\n     Successfully fetched data for {self.successful_fetches} URLs.")
        return results
    
    @classmethod
    async def run(cls, urls_list: List[str], concurrency: int = 10) -> Optional[List[Optional[Dict]]]:
        async with cls(concurrency) as scraper:
            data = await scraper.fetch_data(urls_list)
            if data:
                return data
            else:
                logging.error('No data fetched from the URLs')
                return None
            

if __name__ == "__main__":

    urls = [
        "https://api.open.fec.gov/v1/candidates/search/?page=1&per_page=100&cycle=2022&sort=name&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key=DEMO_KEY",
        "https://api.open.fec.gov/v1/candidates/search/?page=2&per_page=100&cycle=2022&sort=name&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key=DEMO_KEY",
        "https://api.open.fec.gov/v1/candidates/search/?page=3&per_page=100&cycle=2022&sort=name&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key=DEMO_KEY",
    ]
    
    results = asyncio.run(Scraper.run(urls))
