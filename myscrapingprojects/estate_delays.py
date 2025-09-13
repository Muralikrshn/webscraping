from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import pandas as pd
import random


# setting the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class EstateScraper:
  def __init__(self, headless=True):
    """Initializing the driver to none just to use it later"""
    self.driver = None
    self.setup_driver(headless)

  def human_delay(self, min_seconds=1, max_seconds=3):
    """Add random delay to mimic human behavior"""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

  def setup_driver(self, headless):
    """setting up the options to pass to the driver"""
    chrome_options = Options()
    if headless:
      chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Add more human-like browser settings
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    """Creating the driver from webdriver"""
    try:
      chrome_driver = webdriver.Chrome(options=chrome_options)
      self.driver = chrome_driver
      """this line prevents the detection of selenium to the websites"""
      self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
      
      # Human-like delay after driver setup
      self.human_delay(2, 4)

    except Exception as e:
      logger.error(f"Error setting up the driver: {e}")
      raise


  def extract_place_data(self, place_element):
    """extracting data from single place element"""
    data = {}

    # helper for extracting text safely
    def safe_extract(selector, attribute=None, container=place_element):
        try:
            # Small delay before each extraction to mimic human reading time
            time.sleep(random.uniform(0.1, 0.3))
            elem = container.find_element(By.CSS_SELECTOR, selector)
            return elem.get_attribute(attribute) if attribute else elem.text.strip()
        except NoSuchElementException:
            return None 

    # Helper for search text in multiple containers
    def search_in_containers(containers, patterns, extract_func=None):
        for container in containers:
            text = container.text.strip()
            for pattern in patterns:
                if pattern.lower() in text.lower():
                    return extract_func(text) if extract_func else text
        return None
    
    # extract name for the place
    data['name'] = safe_extract("div.qBF1Pd.fontHeadlineSmall")
    data['rating'] = safe_extract("span.MW4etd")
    data['address'] = safe_extract("div.W4Efsd span:nth-of-type(3)")

    return data


  def search_places(self, query, location, max_results=10):
    """let's build the url to search for places"""
    search_query = f"{query} {location}".replace(" ", "+")
    url = f"https://www.google.co.in/maps/search/{search_query}"

    logger.info(f"Searching for: {search_query}")
    
    # Human-like delay before navigation
    self.human_delay(1, 2)
    
    self.driver.get(url)
    
    # Mimic human behavior - wait a bit after page load as humans would
    self.human_delay(3, 5)

    # Wait for the results to load
    try:
      wait = WebDriverWait(self.driver, 15)  # Increased timeout
      wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']")))
      
      # Additional delay after elements are found (human would take time to process)
      self.human_delay(2, 4)

    except TimeoutException:
      logger.error("Timeout waiting for page to load")
      return []
      
    places = []
    seen_places = set()

    # find the scrollable results container
    try:
      results_panel = self.driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
      
      # Small delay after finding the container
      self.human_delay(1, 2)

    except NoSuchElementException:
      logger.error("Could not find the results container")
      return places
    

    previous_count = 0
    no_new_results_count = 0
    scroll_count = 0
    
    while len(places) < max_results:
      # getting the current places elements
      place_elements = results_panel.find_elements(By.CSS_SELECTOR, "div.Nv2PK.tH5CWc.THOPZb ")

      for place_element in place_elements[len(places):]:
        # only process new elements
        if len(places) >= max_results:
          break

        try:
          # Human-like delay before processing each place
          self.human_delay(0.5, 1.5)
          
          place_data = self.extract_place_data(place_element)

          # Avoid duplicates
          place_id = f"{place_data.get('name', '')}_{place_data.get('address', '')}"
          if place_id not in seen_places and place_data.get('name'):
            places.append(place_data)
            seen_places.add(place_id)
            logger.info(f"Scraped: {place_data.get('name', 'Unknown')}")
            
            # Brief pause after successful extraction (human would take time to read/process)
            time.sleep(random.uniform(0.2, 0.5))

        except Exception as e:
          logger.error(f"Error extracting place data: {e}")
          # Even on errors, add a small delay
          time.sleep(random.uniform(0.1, 0.3))
          continue

      # Check if we found new results
      if len(places) == previous_count:
          no_new_results_count += 1
          if no_new_results_count >= 3:  # Stop if no new results after 3 attempts
              logger.info("No more new results found, stopping scroll")
              break
          
          # If no new results, wait a bit longer before trying again
          self.human_delay(2, 4)
      else:
          no_new_results_count = 0

      previous_count = len(places)
      scroll_count += 1
            
      # Scroll down to load more results with human-like behavior
      # Vary scroll amount slightly to mimic human scrolling
      scroll_amount = random.randint(800, 1200)
      self.driver.execute_script(f"arguments[0].scrollTop += {scroll_amount}", results_panel)
      
      # Variable delay after scrolling - humans don't scroll at constant intervals
      if scroll_count % 5 == 0:
          # Longer pause every 5 scrolls (human might take a break to read)
          self.human_delay(5, 8)
      elif scroll_count % 3 == 0:
          # Medium pause every 3 scrolls
          self.human_delay(3, 5)
      else:
          # Regular pause between scrolls
          self.human_delay(2, 4)
      
      logger.info(f"Currently scraped {len(places)} places (Scroll #{scroll_count})")
      
      # Add occasional longer breaks to really mimic human behavior
      if scroll_count % 10 == 0:
          logger.info("Taking a longer break to mimic human behavior...")
          self.human_delay(8, 15)

    return places[:max_results]


  def save_to_csv(self, places, filename):
     """save data to csv using pandas"""
     # Small delay before saving (human would take time to decide on filename, etc.)
     self.human_delay(1, 2)
     
     df = pd.DataFrame(places)
     df.to_csv(filename, index=False)
     logger.info(f"Data saved to {filename}")


  def close(self):
    """Close the browser driver"""
    # Human-like delay before closing (human would take time to close browser)
    self.human_delay(1, 2)
    
    if self.driver:
        self.driver.quit()


def main():
  # Add initial delay to mimic human startup time
  time.sleep(random.uniform(2, 4))
  
  scraper = EstateScraper(headless=False)

  """this is where our scraping starts"""
  try:
    """Search for estates in usa"""
    places = scraper.search_places(
      query="estate planning firm",
      location = "usa",
      max_results=10000
    )

    # print results with human-like delay between each print
    for idx, place in enumerate(places, start=1):
      print(f"{idx}. {place['name']} - {place['address']} - Rating: {place['rating']}")
      # Small delay between prints (human would read each result)
      if idx % 10 == 0:  # Longer pause every 10 results
          time.sleep(random.uniform(1, 2))
      else:
          time.sleep(random.uniform(0.1, 0.3))

    # Finally let's save to csv
    scraper.save_to_csv(places, "estate_planning_firms_usa.csv")

  except Exception as e:
    logger.error(f"Error occurred during scraping: {e}")

  finally:
    # Human-like delay before cleanup
    time.sleep(random.uniform(1, 3))
    scraper.close()


if __name__ == "__main__":
  main()