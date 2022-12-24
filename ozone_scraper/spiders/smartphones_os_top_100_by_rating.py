import scrapy

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from scrapy_selenium import SeleniumRequest
import pandas as pd


class SmartphonesOsTop100ByRatingSpider(scrapy.Spider):
    name = 'smartphones_os_top_100_by_rating'
    options = type('additional_options', (), dict(
        domain = 'https://www.ozon.ru',
        start_url = 'https://www.ozon.ru/category/smartfony-15502/?sorting=rating',
        city = 'Москва',
        elements_list = [],
        depth = 100,
    ))()
    output = pd.DataFrame()

    def get_url(self, link):
        return self.options.domain + link

    def start_requests(self):
        yield SeleniumRequest(
            url=self.options.start_url,
            callback=self.check_location,
            dont_filter=True,
        )

    def check_location(self, response):
        city_current = response.xpath('//div[@data-addressbookbar]//span[text()]')[0]\
            .re('\>(.*?)\<')[0]
        if city_current != self.options.city:
            driver = response.meta['driver']
            driver.find_element(By.XPATH, '//span[text()="Укажите адрес доставки"]').click()
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, '//span[text()="Изменить"]')))
            driver.find_element(By.XPATH, '//span[text()="Изменить"]').click()
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//div[text()='Москва']")))
            try:
                driver.find_element(By.XPATH, f"//div[text()='{self.options.city}']").click()
            except:
                driver.find_element(By.XPATH, "//div[text()='Москва']").click()
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//div[34]/div[2]//a")))
        yield SeleniumRequest(
            url=self.options.start_url,
            wait_time=60,
            wait_until=EC.presence_of_element_located((By.XPATH, '//div[34]/div[2]//a')),
        )

    def parse(self, response):
        if len(self.options.elements_list) < self.options.depth:
            self.options.elements_list += response.xpath(
                "//div[contains(@class, 'widget-search-result-container')]/div/div/div[2]//a")
        if len(self.options.elements_list) < self.options.depth:
            next_button_href = response.xpath('//div[text()="Дальше"]/../../../a')[0].attrib['href']
            url = self.get_url(next_button_href)
            yield SeleniumRequest(
                url=url,
                wait_time=60,
                wait_until=EC.presence_of_element_located((By.XPATH, '//div[34]/div[2]//a'))
            )
        else:
            if len(self.options.elements_list) > self.options.depth:
                self.options.elements_list = self.options.elements_list[:self.options.depth]
                for element in self.options.elements_list:
                    element_href = element.css('a').attrib['href']
                    self.start_urls.append(self.get_url(element_href))
            else:
                os = version = '-'
                os_xpath = '//span[text()="Операционная система"]'
                version_xpath = '//span[contains(.,"Версия")]'
                if response.xpath(version_xpath):
                    version = [
                        response.xpath(version_xpath + '/../..//a').re('\>(.*?)\<'),
                        response.xpath(version_xpath + '/../..//dd').re('\>(.*?)\<'),
                    ]
                    version = version[0] if version[0] else version[1]
                    version = version[0].split(' ')
                    if version:
                        os, version = version[0], version[1]
                        if '.x' in version: version = version[:-2]
                elif response.xpath(os_xpath):
                    os = [
                        response.xpath(os_xpath + '/../..//a').re('\>(.*?)\<'),
                        response.xpath(os_xpath + '/../..//dd').re('\>(.*?)\<'),
                    ]
                    os = os[0][0] if os[0] else os[1][0]
                else:
                    pass
                name = response.css('h1')[0].re('\>(.*?)\<')
                url = response.url.split('/?')[0]
                self.output = self.output.append(
                    pd.DataFrame({
                        "name": name,
                        "OS": [os],
                        "version": [version],
                        "full_version": [f'{os} {version}' if f'{os}{version}' != '--' else '-'],
                        "url": [url],
                    }), ignore_index=True)
            if not self.start_urls:
                self.output.to_csv(f'output/{self.name}.csv')
                self.output.full_version.value_counts().to_csv(f'output/{self.name}.value_counts.csv')
                return
            url = self.start_urls.pop()
            yield SeleniumRequest(
                url=url,
                wait_time=30,
                wait_until=EC.any_of(
                    EC.presence_of_element_located((By.XPATH, "//div[text()='Основные']")),
                    EC.presence_of_element_located((By.XPATH, "//div[text()='Общие']")),
            ))
