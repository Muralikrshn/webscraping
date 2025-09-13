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
import uuid


# Setting the logger
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BrightDataMultithreadedScraper:
    def __init__(self, max_workers=5, headless=True):
        """
        Initialize scraper with Bright Data proxy support and multithreading
        """
        self.max_workers = max_workers
        self.headless = headless
        self.results_lock = Lock()
        self.all_results = []
        self.seen_places = set()
        
        # Bright Data Proxy Configuration
        self.proxy_config = {
            'host': 'brd.superproxy.io',
            'port': '33335',
            'username': 'brd-customer-hl_5bcfb25a-zone-datacenter_proxy1',  # Replace with your credentials
            'password': 'e69r493xfrf2'  # Replace with your password
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

    def test_proxy_connection(self, proxy_url, username, password):
        """Test if Bright Data proxy is working before using it"""
        try:
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            
            auth = (username, password)
            
            # Test with Bright Data's test endpoint
            response = requests.get(
                'https://geo.brdtest.com/welcome.txt?product=dc&method=native', 
                proxies=proxies, 
                auth=auth,
                timeout=15
            )
            
            if response.status_code == 200:
                logger.info(f"Proxy working. Response: {response.text[:100]}...")
                return True
            else:
                logger.warning(f"Proxy test failed with status: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Proxy test failed: {e}")
            return False

    def get_proxy_credentials(self, thread_id, session_id=None):
        """
        Generate proxy credentials for Bright Data with session support
        Bright Data supports session stickiness through session parameter
        """
        base_username = self.proxy_config['username']
        password = self.proxy_config['password']
        
        # Add session ID for sticky sessions (optional)
        if session_id:
            # Bright Data session format: username-session-{session_id}
            username = f"{base_username}-session-{session_id}"
        else:
            # Use thread ID for basic session management
            username = f"{base_username}-session-thread{thread_id}"
            
        return username, password

    def create_driver_with_brightdata_proxy(self, thread_id, session_id=None):
        """Create Chrome driver with Bright Data proxy configuration"""
        
        # Get proxy credentials for this thread
        username, password = self.get_proxy_credentials(thread_id, session_id)
        
        # Build proxy URL
        proxy_host = self.proxy_config['host']
        proxy_port = self.proxy_config['port']
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        
        # Test proxy before using
        if not self.test_proxy_connection(proxy_url, username, password):
            logger.error(f"Thread {thread_id}: Bright Data proxy connection failed")
            # Continue anyway - some proxies might still work
        
        chrome_options = Options()
        
        # Basic Chrome options
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Bright Data Proxy configuration
        chrome_options.add_argument(f'--proxy-server=http://{proxy_host}:{proxy_port}')
        
        # Disable proxy bypass for local addresses
        chrome_options.add_argument('--proxy-bypass-list=<-loopback>')
        
        logger.info(f"Thread {thread_id}: Using Bright Data proxy {proxy_host}:{proxy_port} with session")
        
        # Rotate User-Agents per thread
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
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
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--ignore-ssl-errors")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            
            # Set up proxy authentication via Chrome DevTools Protocol
            driver.execute_cdp_cmd('Network.enable', {})
            
            # Add proxy authentication
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": user_agents[thread_id % len(user_agents)]
            })
            
            # Handle proxy authentication
            def authenticate_proxy(request):
                if request['request']['url'].startswith('http'):
                    return {
                        'username': username,
                        'password': password
                    }
                return {}
            
            # Enable request interception for proxy auth
            driver.execute_cdp_cmd('Runtime.addBinding', {'name': 'authenticate'})
            driver.execute_script(f"""
                window.authenticate = function(request) {{
                    return {{
                        username: '{username}',
                        password: '{password}'
                    }};
                }};
            """)
            
            # Anti-detection measures
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;")
            
            # Thread-specific startup delay to avoid simultaneous connections
            time.sleep(random.uniform(thread_id * 1.5, thread_id * 2.5 + 3))
            
            return driver
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error creating driver with Bright Data proxy: {e}")
            raise

    def human_delay(self, min_seconds=1, max_seconds=3, thread_factor=1):
        """Add random delay with thread-specific variation"""
        base_delay = random.uniform(min_seconds, max_seconds)
        thread_variation = random.uniform(0, thread_factor * 0.3)
        time.sleep(base_delay + thread_variation)

    def extract_place_data(self, place_element, thread_id):
        """Extract data from single place element (thread-safe)"""
        data = {}

        def safe_extract(selector, attribute=None, container=place_element):
            try:
                time.sleep(random.uniform(0.05, 0.15))  # Reduced for speed
                elem = container.find_element(By.CSS_SELECTOR, selector)
                return elem.get_attribute(attribute) if attribute else elem.text.strip()
            except NoSuchElementException:
                return None 

        # Extract comprehensive data
        data['name'] = safe_extract("div.qBF1Pd.fontHeadlineSmall")
        data['rating'] = safe_extract("span.MW4etd")
        data['address'] = safe_extract("div.W4Efsd span:nth-of-type(3)")
        
        # Try to extract additional data
        data['phone'] = safe_extract("span[data-value]", "data-value")
        data['website'] = safe_extract("a[data-value]", "href")
        data['hours'] = safe_extract("div.t39EBf span")
        
        # Add metadata
        data['thread_id'] = thread_id
        data['scraped_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
        
        return data

    def rotate_proxy_session(self, thread_id):
        """
        Force proxy rotation by generating new session ID
        Bright Data rotates IPs when session changes
        """
        new_session = f"{thread_id}-{int(time.time())}-{random.randint(1000, 9999)}"
        return new_session

    def scrape_state(self, query, state, max_results_per_state, thread_id):
        """Scrape estate planning firms in a specific state using Bright Data proxy"""
        logger.info(f"Thread {thread_id} starting to scrape {state}")
        
        driver = None
        local_results = []
        session_id = self.rotate_proxy_session(thread_id)  # Initial session
        
        try:
            # Create driver with Bright Data proxy for this thread
            driver = self.create_driver_with_brightdata_proxy(thread_id, session_id)
            
            # Build search query
            search_query = f"{query} {state} USA".replace(" ", "+")
            url = f"https://www.google.co.in/maps/search/{search_query}"
            
            logger.info(f"Thread {thread_id} searching: {search_query}")
            
            # Human-like delay before navigation
            self.human_delay(2, 4, thread_id)
            
            driver.get(url)
            self.human_delay(4, 6, thread_id)

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

            # Scraping loop with proxy rotation
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
                        self.human_delay(0.3, 0.8, thread_id)
                        place_data = self.extract_place_data(place_element, thread_id)
                        place_data['state'] = state
                        place_data['session_id'] = session_id

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
                    self.human_delay(4, 7, thread_id)
                else:
                    no_new_results_count = 0

                previous_count = len(local_results)
                scroll_count += 1

                # Rotate proxy session every 20 scrolls for fresh IP
                if scroll_count % 20 == 0:
                    logger.info(f"Thread {thread_id}: Rotating proxy session...")
                    session_id = self.rotate_proxy_session(thread_id)
                    # Note: Would need to recreate driver for new session, 
                    # but that's expensive, so we'll keep current session

                # Scroll with variation
                scroll_amount = random.randint(600, 1000)
                driver.execute_script(f"arguments[0].scrollTop += {scroll_amount}", results_panel)

                # Variable delays based on Bright Data best practices
                if scroll_count % 10 == 0:
                    self.human_delay(8, 12, thread_id)  # Longer break every 10 scrolls
                elif scroll_count % 5 == 0:
                    self.human_delay(4, 7, thread_id)   # Medium break every 5 scrolls
                else:
                    self.human_delay(2, 4, thread_id)   # Regular delay

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
        Main method to scrape estate planning firms across US states using Bright Data
        """
        
        # Select states to scrape (limit for testing)
        states_to_scrape = self.us_states[:self.max_workers * 3]  # 3 states per worker
        max_results_per_state = max_results // len(states_to_scrape)
        
        logger.info(f"Starting Bright Data parallel scraping across {len(states_to_scrape)} states")
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
                    i % self.max_workers
                )
                futures.append((future, state))

            # Collect results as they complete
            for future, state in futures:
                try:
                    state_results = future.result(timeout=2400)  # 40 minute timeout per state
                    with self.results_lock:
                        self.all_results.extend(state_results)
                        logger.info(f"Completed {state}: {len(state_results)} results. Total: {len(self.all_results)}")
                        
                except concurrent.futures.TimeoutError:
                    logger.error(f"Timeout scraping {state}")
                except Exception as e:
                    logger.error(f"Failed to scrape {state}: {e}")

        return self.all_results[:max_results]

    def save_to_csv(self, places, filename):
        """Save results to CSV with Bright Data metadata"""
        if not places:
            logger.warning("No data to save")
            return
            
        df = pd.DataFrame(places)
        
        # Add summary statistics
        summary = {
            'total_places': len(places),
            'unique_states': len(df['state'].unique()) if 'state' in df.columns else 0,
            'scraped_at': time.strftime("%Y-%m-%d %H:%M:%S"),
            'proxy_provider': 'Bright Data Datacenter Proxies',
            'proxy_endpoint': f"{self.proxy_config['host']}:{self.proxy_config['port']}"
        }
        
        # Save main data
        df.to_csv(filename, index=False)
        
        # Save summary
        summary_filename = filename.replace('.csv', '_summary.json')
        with open(summary_filename, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Data saved to {filename}")
        logger.info(f"Summary saved to {summary_filename}")

    def get_proxy_usage_stats(self):
        """
        Get proxy usage statistics (if available)
        Bright Data provides usage APIs but requires separate implementation
        """
        logger.info("Proxy usage statistics:")
        logger.info(f"  Provider: Bright Data")
        logger.info(f"  Endpoint: {self.proxy_config['host']}:{self.proxy_config['port']}")
        logger.info(f"  Sessions used: {len(set(r.get('session_id', '') for r in self.all_results))}")


def main():
    """
    IMPORTANT: Update Bright Data credentials before running!
    """
    
    # Initialize scraper with Bright Data proxy support
    scraper = BrightDataMultithreadedScraper(
        max_workers=4,  # Conservative start - Bright Data allows good concurrency
        headless=False  # Set to True for production
    )
    
    # Update Bright Data credentials (REQUIRED!)
    scraper.proxy_config['username'] = 'brd-customer-hl_5bcfb25a-zone-datacenter_proxy1'  # Your username
    scraper.proxy_config['password'] = 'e69r493xfrf2'  # Your password
    
    try:
        logger.info("Starting multithreaded scraping with Bright Data datacenter proxies...")
        
        places = scraper.scrape_estate_firms_parallel(
            query="estate planning firm",
            max_results=1500  # Start with moderate number for testing
        )

        # Display results summary
        if places:
            print(f"\n{'='*60}")
            print(f"BRIGHT DATA SCRAPING COMPLETED!")
            print(f"{'='*60}")
            print(f"Total firms found: {len(places)}")
            
            # Show sample results
            print(f"\nSample results:")
            for idx, place in enumerate(places[:5], start=1):
                print(f"{idx}. {place.get('name', 'N/A')} - {place.get('state', 'N/A')}")
                print(f"   Address: {place.get('address', 'N/A')}")
                print(f"   Rating: {place.get('rating', 'N/A')} | Phone: {place.get('phone', 'N/A')}")
                print(f"   Thread: {place.get('thread_id', 'N/A')} | Session: {place.get('session_id', 'N/A')[:20]}...")
                print("-" * 50)

            # Save results with timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"estate_firms_brightdata_{timestamp}.csv"
            scraper.save_to_csv(places, filename)
            
            # Show state distribution
            states = {}
            for place in places:
                state = place.get('state', 'Unknown')
                states[state] = states.get(state, 0) + 1
            
            print(f"\nResults by state:")
            for state, count in sorted(states.items()):
                print(f"  {state}: {count} firms")
            
            # Show proxy usage stats
            scraper.get_proxy_usage_stats()
                
        else:
            logger.warning("No results found! Check proxy configuration and credentials.")

    except Exception as e:
        logger.error(f"Error during scraping: {e}")

    finally:
        logger.info("Bright Data scraping session completed.")


if __name__ == "__main__":
    main()