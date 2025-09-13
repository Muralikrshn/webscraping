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
import threading
import concurrent.futures
from threading import Lock
import requests
import json


# Setting the logger
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProxyMultithreadedEstateScraper:
    def __init__(self, max_workers=5, headless=True):
        """
        Initialize scraper with proxy support and multithreading
        """
        self.max_workers = max_workers
        self.headless = headless
        self.results_lock = Lock()
        self.all_results = []
        self.seen_places = set()
        
        # DataImpulse Proxy Configuration
        self.proxy_config = {
            'username': 'YOUR_USERNAME',  # Replace with your DataImpulse username
            'password': 'YOUR_PASSWORD',  # Replace with your DataImpulse password
            'endpoint': 'gw.dataimpulse.com:12345'
        }
        
        # US States for parallel processing
        self.us_states = [
            'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
            'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
            'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
            'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
            'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
            'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
            'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
            'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
            'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
            'West Virginia', 'Wisconsin', 'Wyoming'
        ]

    def test_proxy_connection(self, proxy_url):
        """Test if proxy is working before using it"""
        try:
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            
            # Test with a simple request
            response = requests.get(
                'http://httpbin.org/ip', 
                proxies=proxies, 
                timeout=10
            )
            
            if response.status_code == 200:
                ip_info = response.json()
                logger.info(f"Proxy working. Current IP: {ip_info.get('origin')}")
                return True
            else:
                logger.warning(f"Proxy test failed with status: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Proxy test failed: {e}")
            return False

    def get_proxy_url(self, session_id=None):
        """
        Generate proxy URL for DataImpulse
        session_id helps maintain the same IP for a session if needed
        """
        username = self.proxy_config['username']
        password = self.proxy_config['password']
        endpoint = self.proxy_config['endpoint']
        
        # Add session ID for sticky sessions (optional)
        if session_id:
            username = f"{username}-session-{session_id}"
            
        proxy_url = f"http://{username}:{password}@{endpoint}"
        return proxy_url

    def create_driver_with_proxy(self, thread_id, session_id=None):
        """Create Chrome driver with DataImpulse proxy configuration"""
        
        # Get proxy URL for this thread
        proxy_url = self.get_proxy_url(session_id or thread_id)
        
        # Test proxy before using
        if not self.test_proxy_connection(proxy_url):
            logger.error(f"Thread {thread_id}: Proxy connection failed, using direct connection")
            proxy_url = None
        
        chrome_options = Options()
        
        # Basic Chrome options
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Proxy configuration
        if proxy_url:
            # Extract proxy details
            proxy_parts = proxy_url.replace('http://', '').split('@')
            auth_part = proxy_parts[0]  # username:password
            proxy_endpoint = proxy_parts[1]  # gw.dataimpulse.com:12345
            
            # Set proxy in Chrome
            chrome_options.add_argument(f'--proxy-server=http://{proxy_endpoint}')
            
            logger.info(f"Thread {thread_id}: Using proxy {proxy_endpoint}")
        
        # Rotate User-Agents per thread
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        ]
        chrome_options.add_argument(f"--user-agent={user_agents[thread_id % len(user_agents)]}")
        
        # Random window sizes
        window_sizes = ["1920,1080", "1366,768", "1440,900", "1536,864", "1280,720"]
        chrome_options.add_argument(f"--window-size={window_sizes[thread_id % len(window_sizes)]}")
        
        # Additional stealth options
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins-discovery")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            
            # If using proxy with authentication, set up authentication
            if proxy_url and '@' in proxy_url:
                auth_part = proxy_url.split('@')[0].replace('http://', '')
                username, password = auth_part.split(':')
                
                # Enable proxy authentication
                driver.execute_cdp_cmd('Network.enable', {})
                driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                    "userAgent": user_agents[thread_id % len(user_agents)]
                })
            
            # Anti-detection measures
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;")
            
            # Thread-specific startup delay
            time.sleep(random.uniform(thread_id * 2, thread_id * 2 + 4))
            
            return driver
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error creating driver with proxy: {e}")
            raise

    def human_delay(self, min_seconds=1, max_seconds=3, thread_factor=1):
        """Add random delay with thread-specific variation"""
        base_delay = random.uniform(min_seconds, max_seconds)
        thread_variation = random.uniform(0, thread_factor * 0.5)
        time.sleep(base_delay + thread_variation)

    def extract_place_data(self, place_element, thread_id):
        """Extract data from single place element (thread-safe)"""
        data = {}

        def safe_extract(selector, attribute=None, container=place_element):
            try:
                time.sleep(random.uniform(0.05, 0.2))  # Reduced delay for speed
                elem = container.find_element(By.CSS_SELECTOR, selector)
                return elem.get_attribute(attribute) if attribute else elem.text.strip()
            except NoSuchElementException:
                return None 

        # Extract basic data
        data['name'] = safe_extract("div.qBF1Pd.fontHeadlineSmall")
        data['rating'] = safe_extract("span.MW4etd")
        data['address'] = safe_extract("div.W4Efsd span:nth-of-type(3)")
        data['thread_id'] = thread_id
        data['scraped_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
        
        return data

    def scrape_state(self, query, state, max_results_per_state, thread_id):
        """Scrape estate planning firms in a specific state"""
        logger.info(f"Thread {thread_id} starting to scrape {state}")
        
        driver = None
        local_results = []
        
        try:
            # Create driver with proxy for this thread
            driver = self.create_driver_with_proxy(thread_id, session_id=f"state-{state}")
            
            # Build search query
            search_query = f"{query} {state} USA".replace(" ", "+")
            url = f"https://www.google.co.in/maps/search/{search_query}"
            
            logger.info(f"Thread {thread_id} searching: {search_query}")
            
            # Human-like delay before navigation
            self.human_delay(2, 5, thread_id)
            
            driver.get(url)
            self.human_delay(4, 7, thread_id)

            # Wait for results to load
            try:
                wait = WebDriverWait(driver, 25)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']")))
                self.human_delay(2, 4, thread_id)
                
            except TimeoutException:
                logger.error(f"Thread {thread_id}: Timeout waiting for {state} results")
                return []

            # Find results container
            try:
                results_panel = driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
                self.human_delay(1, 2, thread_id)
                
            except NoSuchElementException:
                logger.error(f"Thread {thread_id}: Could not find results for {state}")
                return []

            # Scraping loop
            previous_count = 0
            no_new_results_count = 0
            scroll_count = 0
            
            while len(local_results) < max_results_per_state:
                place_elements = results_panel.find_elements(By.CSS_SELECTOR, "div.Nv2PK.tH5CWc.THOPZb")

                # Process new elements
                for place_element in place_elements[len(local_results):]:
                    if len(local_results) >= max_results_per_state:
                        break

                    try:
                        self.human_delay(0.3, 1, thread_id)
                        place_data = self.extract_place_data(place_element, thread_id)
                        place_data['state'] = state  # Add state info

                        if place_data.get('name'):
                            place_id = f"{place_data.get('name', '')}_{place_data.get('address', '')}_{state}"
                            
                            # Thread-safe duplicate checking
                            with self.results_lock:
                                if place_id not in self.seen_places:
                                    local_results.append(place_data)
                                    self.seen_places.add(place_id)
                                    logger.info(f"Thread {thread_id} ({state}): {place_data.get('name', 'Unknown')}")

                    except Exception as e:
                        logger.error(f"Thread {thread_id} extraction error: {e}")
                        continue

                # Check for new results
                if len(local_results) == previous_count:
                    no_new_results_count += 1
                    if no_new_results_count >= 3:
                        logger.info(f"Thread {thread_id}: No more results for {state}")
                        break
                    self.human_delay(4, 8, thread_id)
                else:
                    no_new_results_count = 0

                previous_count = len(local_results)
                scroll_count += 1

                # Scroll with variation
                scroll_amount = random.randint(600, 1000)
                driver.execute_script(f"arguments[0].scrollTop += {scroll_amount}", results_panel)

                # Variable delays
                if scroll_count % 8 == 0:
                    self.human_delay(8, 12, thread_id)
                elif scroll_count % 4 == 0:
                    self.human_delay(4, 6, thread_id)
                else:
                    self.human_delay(2, 4, thread_id)

                logger.info(f"Thread {thread_id} ({state}): {len(local_results)} places (Scroll #{scroll_count})")

            return local_results

        except Exception as e:
            logger.error(f"Thread {thread_id} ({state}) error: {e}")
            return []
            
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    logger.error(f"Error closing driver for thread {thread_id}: {e}")

    def scrape_estate_firms_parallel(self, query="estate planning firm", max_results=5000):
        """
        Main method to scrape estate planning firms across US states in parallel
        """
        
        # Distribute states across threads
        states_to_scrape = self.us_states[:self.max_workers * 2]  # Limit states for testing
        max_results_per_state = max_results // len(states_to_scrape)
        
        logger.info(f"Starting parallel scraping across {len(states_to_scrape)} states")
        logger.info(f"Using {self.max_workers} threads")
        logger.info(f"Target: {max_results_per_state} results per state")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Create futures for each state
            futures = []
            
            for i, state in enumerate(states_to_scrape):
                future = executor.submit(
                    self.scrape_state, 
                    query, 
                    state, 
                    max_results_per_state, 
                    i % self.max_workers  # Distribute thread IDs
                )
                futures.append((future, state))

            # Collect results as they complete
            for future, state in futures:
                try:
                    state_results = future.result(timeout=1800)  # 30 minute timeout per state
                    with self.results_lock:
                        self.all_results.extend(state_results)
                        logger.info(f"Completed {state}: {len(state_results)} results. Total: {len(self.all_results)}")
                        
                except concurrent.futures.TimeoutError:
                    logger.error(f"Timeout scraping {state}")
                except Exception as e:
                    logger.error(f"Failed to scrape {state}: {e}")

        return self.all_results[:max_results]

    def save_to_csv(self, places, filename):
        """Save results to CSV with additional metadata"""
        if not places:
            logger.warning("No data to save")
            return
            
        df = pd.DataFrame(places)
        
        # Add summary statistics
        summary = {
            'total_places': len(places),
            'unique_states': len(df['state'].unique()) if 'state' in df.columns else 0,
            'scraped_at': time.strftime("%Y-%m-%d %H:%M:%S"),
            'proxy_used': 'DataImpulse Residential Proxies'
        }
        
        # Save main data
        df.to_csv(filename, index=False)
        
        # Save summary
        summary_filename = filename.replace('.csv', '_summary.json')
        with open(summary_filename, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Data saved to {filename}")
        logger.info(f"Summary saved to {summary_filename}")


def main():
    """
    IMPORTANT: Update proxy credentials before running!
    """
    
    # Initialize scraper with proxy support
    scraper = ProxyMultithreadedEstateScraper(
        max_workers=5,  # Adjust based on your proxy plan
        headless=False  # Set to True for production
    )
    
    # Update proxy credentials (REQUIRED!)
    scraper.proxy_config['username'] = 'YOUR_DATAIMPULSE_USERNAME'  # Replace this!
    scraper.proxy_config['password'] = 'YOUR_DATAIMPULSE_PASSWORD'  # Replace this!
    
    try:
        logger.info("Starting multithreaded scraping with DataImpulse proxies...")
        
        places = scraper.scrape_estate_firms_parallel(
            query="estate planning firm",
            max_results=2000  # Start with smaller number for testing
        )

        # Display results summary
        if places:
            print(f"\n{'='*50}")
            print(f"SCRAPING COMPLETED!")
            print(f"{'='*50}")
            print(f"Total firms found: {len(places)}")
            
            # Show sample results
            print(f"\nSample results:")
            for idx, place in enumerate(places[:5], start=1):
                print(f"{idx}. {place.get('name', 'N/A')} - {place.get('state', 'N/A')} - Rating: {place.get('rating', 'N/A')}")

            # Save results
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"estate_planning_firms_usa_{timestamp}.csv"
            scraper.save_to_csv(places, filename)
            
            # Show state distribution
            states = {}
            for place in places:
                state = place.get('state', 'Unknown')
                states[state] = states.get(state, 0) + 1
            
            print(f"\nResults by state:")
            for state, count in sorted(states.items()):
                print(f"  {state}: {count} firms")
                
        else:
            logger.warning("No results found!")

    except Exception as e:
        logger.error(f"Error during scraping: {e}")

    finally:
        logger.info("Scraping completed.")


if __name__ == "__main__":
    main()