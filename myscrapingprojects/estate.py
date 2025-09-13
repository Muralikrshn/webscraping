


from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import pandas as pd


# setting the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class EstateScraper:
  def __init__(self, headless=True):
    """Initializing the driver to none just to use it later"""
    self.driver = None
    self.setup_driver(headless)

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

    """Creaing the driver from webdriver"""
    try:
      chrome_driver = webdriver.Chrome(options=chrome_options)
      self.driver = chrome_driver
      """this lne prevents the detection o selenium to the websites"""
      self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    except Exception as e:
      logger.error(f"Error setting up the driver: {e}")
      raise


  def extract_place_data(self, place_element):
    """extracting data from single place element"""
    data = {}

    # helper for extracting text safely
    def safe_extract(selector, attribute=None, container=place_element):
        try:
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
    self.driver.get(url)
    # logger.info(f"Status Code: {response.status_code}")

    # Wait for the results to load
    try:
      wait = WebDriverWait(self.driver, 10)
      wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']")))

    except TimeoutException:
      logger.error("Timeout waiting for page to load")
      return []
      
    places = []
    seen_places = set()

    # find the scrollable results container
    try:
      results_panel = self.driver.find_element(By.CSS_SELECTOR, "div[role='feed']")


    except NoSuchElementException:
      logger.error("Could not find the results container")
      return places
    

    previous_count = 0
    no_new_results_count = 0
    
    while len(places) < max_results:
      # getting the current places elements
      place_elements = results_panel.find_elements(By.CSS_SELECTOR, "div.Nv2PK.tH5CWc.THOPZb ")

      for place_element in place_elements[len(places):]:
        # only process new elements
        if len(places) >= max_results:
          break

        try:
          place_data = self.extract_place_data(place_element)

          # Avoid duplicates
          place_id = f"{place_data.get('name', '')}_{place_data.get('address', '')}"
          if place_id not in seen_places and place_data.get('name'):
            places.append(place_data)
            seen_places.add(place_id)
            logger.info(f"Scraped: {place_data.get('name', 'Unknown')}")

        except Exception as e:
          logger.error(f"Error extracting place data: {e}")
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


  def save_to_csv(self, places, filename):
     """save data to csv using pandas"""
     df = pd.DataFrame(places)
     df.to_csv(filename, index=False)
     logger.info(f"Data saved to {filename}")



  def close(self):
    """Close the browser driver"""
    if self.driver:
        self.driver.quit()








def main():
  scraper = EstateScraper(headless=False)

  """this is where our scraping states"""
  try:
    """Search for estates in usa"""
    places = scraper.search_places(
      query="estate planning firm",
      location = "usa",
      max_results=10000
    )

    # print results
    for idx, place in enumerate(places, start=1):
      print(f"{idx}. {place['name']} - {place['address']} - Rating: {place['rating']}")


    # Finally let's save to csv
    scraper.save_to_csv(places, "estate_planning_firms_usa.csv")

  except Exception as e:
    logger.error(f"Error occurred during scraping: {e}")

  finally:
    scraper.close()






if __name__ == "__main__":
  main()




