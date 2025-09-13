#!/usr/bin/env python3
"""
Google Maps Scraper using Selenium
Scrapes business information from Google Maps search results

Required packages:
pip install selenium beautifulsoup4 pandas
"""

import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleMapsScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with Chrome driver options"""
        self.driver = None
        self.setup_driver(headless)
        
    def setup_driver(self, headless=True):
        """Set up Chrome driver with appropriate options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def search_places(self, query, location="", max_results=20):
        """
        Search for places on Google Maps
        
        Args:
            query (str): Search query (e.g., "restaurants", "hotels")
            location (str): Location to search in (e.g., "New York, NY")
            max_results (int): Maximum number of results to scrape
        
        Returns:
            list: List of dictionaries containing place information
        """
        search_query = f"{query} {location}".strip()
        url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
        
        logger.info(f"Searching for: {search_query}")
        self.driver.get(url)
        
        # Wait for the results to load
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
            )
        except TimeoutException:
            logger.error("Search results did not load in time")
            return []
        
        places = []
        seen_places = set()
        
        # Find the scrollable results container
        try:
            results_panel = self.driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
        except NoSuchElementException:
            logger.error("Could not find results panel")
            return []
        
        previous_count = 0
        no_new_results_count = 0
        
        while len(places) < max_results:
            # Get current place elements - they are divs with class containing "Nv2PK"
            place_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK.THOPZb.CpccDe")
            
            for element in place_elements[len(places):]:  # Only process new elements
                if len(places) >= max_results:
                    break
                    
                try:
                    place_data = self.extract_place_data(element)
                    
                    # Avoid duplicates
                    place_id = f"{place_data.get('name', '')}_{place_data.get('address', '')}"
                    if place_id not in seen_places and place_data.get('name'):
                        places.append(place_data)
                        seen_places.add(place_id)
                        logger.info(f"Scraped: {place_data.get('name', 'Unknown')}")
                        
                except Exception as e:
                    logger.warning(f"Error extracting place data: {e}")
                    continue
            
            # Check if we found new results
            if len(places) == previous_count:
                no_new_results_count += 1
                if no_new_results_count >= 3:  # Stop if no new results after 3 attempts
                    logger.info("No more new results found, stopping scroll")
                    break
            else:
                no_new_results_count = 0
            
            previous_count = len(places)
            
            # Scroll down to load more results
            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_panel)
            time.sleep(3)  # Longer wait for content to load
            
            logger.info(f"Currently scraped {len(places)} places")
        
        return places[:max_results]
    
    def extract_place_data(self, element):
        """Extract data from a single place element based on the actual HTML structure"""
        data = {}
        
        # Helper function to safely extract text from elements
        def safe_extract(selector, attribute=None, container=element):
            try:
                elem = container.find_element(By.CSS_SELECTOR, selector)
                return elem.get_attribute(attribute) if attribute else elem.text.strip()
            except NoSuchElementException:
                return None
        
        # Helper function to search text in multiple containers
        def search_in_containers(containers, patterns, extract_func=None):
            for container in containers:
                text = container.text.strip()
                for pattern in patterns:
                    if pattern.lower() in text.lower():
                        return extract_func(text) if extract_func else text
            return None
        
        # Extract name
        data['name'] = safe_extract(".qBF1Pd.fontHeadlineSmall")
        
        # Extract rating and reviews from aria-label
        rating_elem = safe_extract("span[role='img'][aria-label*='stars']", "aria-label")
        if rating_elem:
            # Parse "4.8 stars 1,459 Reviews"
            parts = rating_elem.split()
            data['rating'] = parts[0] if parts else None
            
            # Extract review count
            if 'Reviews' in rating_elem:
                reviews_part = rating_elem.split('Reviews')[0].split()[-1]
                data['reviews_count'] = reviews_part.replace('(', '').replace(')', '').replace(',', '')
            else:
                data['reviews_count'] = None
        else:
            data['rating'] = None
            data['reviews_count'] = None
        
        # Get all W4Efsd containers for processing
        try:
            info_containers = element.find_elements(By.CSS_SELECTOR, ".W4Efsd")
        except NoSuchElementException:
            info_containers = []
        
        # Extract address (contains street indicators)
        street_indicators = ['st', 'ave', 'blvd', 'rd', 'drive', 'lane', 'way']
        data['address'] = search_in_containers(
            info_containers, 
            street_indicators,
            lambda text: text.split('·')[-1].strip() if '·' in text else text
        )
        
        # Extract price range (contains $ symbols)
        def extract_price(text):
            if '$' in text and '·' in text:
                parts = text.split('·')
                for part in parts:
                    if '$' in part.strip():
                        return part.strip()
            return text if '$' in text else None
        
        data['price_range'] = search_in_containers(info_containers, ['$'], extract_price)
        
        # Extract category (usually first item)
        try:
            category_container = element.find_element(By.CSS_SELECTOR, ".W4Efsd .W4Efsd")
            category_text = category_container.text.strip()
            data['category'] = category_text.split('·')[0].strip() if '·' in category_text else category_text
        except NoSuchElementException:
            data['category'] = None
        
        # Extract hours/status
        data['hours_status'] = search_in_containers(info_containers, ['open', 'closed'])
        
        # Extract description (longer text that doesn't match other patterns)
        description = None
        skip_patterns = ['st ', 'ave ', 'blvd ', 'rd ', 'open', 'closed', 'coffee shop', 'cafe', '·']
        for container in info_containers:
            text = container.text.strip()
            if len(text) > 10 and not any(pattern in text.lower() for pattern in skip_patterns):
                description = text
                break
        data['description'] = description
        
        # Extract Google Maps URL
        data['google_url'] = safe_extract("a.hfpxzc", "href")
        
        return data
    
    def save_to_csv(self, data, filename):
        """Save scraped data to CSV file"""
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logger.info(f"Data saved to {filename}")
    
    def close(self):
        """Close the browser driver"""
        if self.driver:
            self.driver.quit()

def main():
    """Example usage of the GoogleMapsScraper"""
    scraper = GoogleMapsScraper(headless=False)  # Set to True for headless mode
    
    try:
        # Example: Search for coffee shops in Seattle
        places = scraper.search_places(
            query="coffee shops",
            location="Seattle, WA",
            max_results=25
        )
        
        # Print results
        for i, place in enumerate(places, 1):
            print(f"\n{i}. {place.get('name', 'N/A')}")
            print(f"   Rating: {place.get('rating', 'N/A')}")
            print(f"   Reviews: {place.get('reviews_count', 'N/A')}")
            print(f"   Address: {place.get('address', 'N/A')}")
            print(f"   Price: {place.get('price_range', 'N/A')}")
            print(f"   Category: {place.get('category', 'N/A')}")
            print(f"   Hours: {place.get('hours_status', 'N/A')}")
            print(f"   Description: {place.get('description', 'N/A')}")
            print(f"   Google URL: {place.get('google_url', 'N/A')}")
        
        # Save to CSV
        scraper.save_to_csv(places, "google_maps_results.csv")
        
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()