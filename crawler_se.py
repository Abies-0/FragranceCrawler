from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
from selenium.webdriver.common.by import By
from get_config import Config
import time
import pickle

class CrawlerSE:

    def __init__(self):
        self.config = Config("selenium", "crawler.yaml").get()
        self.opts = Options()
        self.opts.add_argument("-profile")
        self.opts.add_argument(self.config["driver"])
        self.opts.add_argument("--incognito")
        self.opts.add_argument("disable-extensions")
        self.opts.add_argument("disable-popup-blocking")
        self.opts.add_argument('--no-sandbox')
        self.opts.add_argument('--disable-dev-shm-usage')
        self.opts.add_argument("user-agent=%s" % (self.config["user_agent"]))
        self.opts.add_experimental_option('detach', True)

    def driver(self, url):
        driver = webdriver.Chrome(options=self.opts)
        driver.get(url)
        driver.implicitly_wait(10)
        driver.maximize_window()
        return driver

    def crawl(self, brand):
        url = "%s%s" % (self.config["url"], brand.lower().strip().replace(" ", "%20"))
        driver = self.driver(url)
        while True:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                btn = driver.find_element(By.XPATH, self.config["btn_xpath"])
                if btn:
                    if btn.is_enabled() == False:
                        print("btn enabled: {}".format(btn.is_enabled()))
                        btn = None
                        break
                    else:
                        print("btn enabled: {}".format(btn.is_enabled()))
                        btn.click()
                        time.sleep(2)
                        btn = None
                else:
                    break
            except Exception as e:
                print(e)
                continue
        html = bs(driver.page_source, 'lxml')
        driver.quit()
        hrefs = html.find("div", id="app").find("div", id="main-content").find("div", class_="ais-InstantSearch").find("div", class_="ais-InfiniteHits").find_all("a")
        pair = {}
        for i in hrefs:
            pair[i.text.replace("\n", "").strip()] = i["href"]
        with open("./data/%s.pkl" % (brand.strip().replace(" ", "")), 'wb') as f:
            pickle.dump(pair, f)
        return pair
