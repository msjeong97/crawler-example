import sys
import re
import requests
import urllib3
import pandas as pd

from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


BASE_URL = "https://www.gsmarena.com"
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Host": "www.gsmarena.com",
    "Referer": "https://www.google.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
}
FILE_PATH = "raw-device-info.csv"

TARGET_VENDORS = ["samsung", "apple"]
REQUEST_LIMIT = 2


def get_request_through_proxy(
        url: str,
        token: str,
        headers: dict = None
) -> requests.Response:
    """
    Send a GET request through a proxy server.

    Args:
        url (str): The URL to send the request to.
        token (str): The authentication token for the proxy server.
        headers (dict, optional): Headers to include in the request.

    Returns:
        Response: The response object from the request.
    """
    proxy_url = f"http://{token}:@proxy.scrape.do:8080"
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }

    if not hasattr(get_request_through_proxy, 'call_count'):
        get_request_through_proxy.call_count = 0

    get_request_through_proxy.call_count += 1

    print(f"Getting request from {url} through proxy {proxy_url}; proxy call count: {get_request_through_proxy.call_count}")
    return requests.get(url, headers=headers, proxies=proxies, verify=False)


def get_vendor_root_urls(token: str) -> list[str]:
    """
    Get the root URLs of vendors from the GSM Arena website.
    Number of proxy requests: 1

    Args:
        token (str): The authentication token for the proxy server.

    Returns:
        list[str]: A list of vendor root URLs. e.g. "https://www.gsmarena.com/samsung-phones-9.php"
    """
    response = get_request_through_proxy(BASE_URL, token, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    vendor_root_urls = list()
    for li_tag in soup.find("div", {"class": "brandmenu-v2"}).find("ul").find_all("li"):
        vendor_root_url = f"{BASE_URL}/{li_tag.a['href']}"

        if vendor_root_url.split(f"{BASE_URL}/")[1].split("-")[0] in TARGET_VENDORS:
            vendor_root_urls.append(vendor_root_url)

    return vendor_root_urls


def check_php_pagination(main_string: str) -> bool:
    """
    Check if the main string contains pagination in the format "p<number>.php".
    Args:
        main_string (str): The string to check for pagination.
    Returns:
        bool: True if pagination is found, False otherwise.
    """
    pattern = r"p\d+\.php"
    return bool(re.search(pattern, main_string))


def generate_pagination_url(start_url: str, end_url: str) -> list[str]:
    """
    Generate a list of pagination URLs between the start and end URLs.
    Args:
        start_url (str): The starting URL, e.g. "https://www.gsmarena.com/samsung-phones-f-9-0-p2.php".
        end_url (str): The ending URL, e.g. "https://www.gsmarena.com/samsung-phones-f-9-0-p28.php".
    Returns:
        list[str]: A list of generated pagination URLs.
    """
    pattern = r"p(\d+)\.php"

    match_start = re.search(pattern, start_url)
    if not match_start:
        raise ValueError(f"Exception: {start_url} doest not match the pattern {pattern}")
    start_page_num = int(match_start.group(1))

    match_end = re.search(pattern, end_url)
    if not match_end:
        raise ValueError(f"Exception: {end_url} doest not match the pattern {pattern}")
    end_page_num = int(match_end.group(1))

    prefix_match = re.search(r"^(.*)p\d+\.php$", start_url)
    if not prefix_match:
        raise ValueError(f"Exception: {start_url} does not match the expected pagination format")

    prefix = prefix_match.group(1)  # e.g. "samsung-phones-f-9-0-"

    all_urls = []

    for page_num in range(start_page_num, end_page_num + 1):
        generated_url = f"{BASE_URL}/{prefix}p{page_num}.php"
        all_urls.append(generated_url)

    return all_urls


def get_all_pagination_urls(url: str, token: str) -> list[str]:
    """
    Get all pagination URLs from the given base URL.

    Args:
        base_url (str): The base URL to start from, e.g. "https://www.gsmarena.com/samsung-phones-9.php".
        token (str): The authentication token for the proxy server.
        Number of proxy requests: 1

    Returns:
        list[str]: A list of all pagination URLs found on the page.
    """
    pagination_urls = list()
    pagination_urls.append(url)
    pagination = set()

    response = get_request_through_proxy(url, token, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    for a in soup.find("div", {"class": "nav-pages"}).find_all("a"):
        href = a["href"]
        if check_php_pagination(href):
            pagination.add(href)

    if pagination and len(pagination) == 2:
        url_list = sorted(list(pagination))
        generated_pagination_urls = generate_pagination_url(url_list[0], url_list[1])
        pagination_urls.extend(generated_pagination_urls)
    else:
        raise Exception(f"Pagination URLs not found or not in expected format: {pagination}")

    return pagination_urls


def get_vendor_urls(vendor_root_urls: list[str], token: str) -> list[str]:
    """
    Get the URLs of vendors from their root pages.
    Number of proxy requests: len(vendor_root_urls)

    Args:
        vendor_root_urls (list): A list of vendor root URLs.
        token (str): The authentication token for the proxy server.

    Returns:
        list: A list of vendor URLs. e.g. "https://www.gsmarena.com/samsung-phones-f-9-0-p6.php"
    """
    vendor_urls = list()

    for vendor_root_url in vendor_root_urls:
        vendor_urls.extend(get_all_pagination_urls(vendor_root_url, token))

    return vendor_urls


def get_device_page_urls(vendor_urls: list[str], token: str) -> list[str]:
    """
    Get the device page URLs from the vendor URLs.
    Number of proxy requests: len(vendor_urls)
    Args:
        vendor_urls (list): A list of vendor URLs.
        token (str): The authentication token for the proxy server.
    Returns:
        list: A list of device page URLs. e.g. "https://www.gsmarena.com/samsung_galaxy_a25-12555.php"
    """
    device_page_urls = list()

    for vendor_url in vendor_urls:
        response = get_request_through_proxy(vendor_url, token, headers=HEADERS)
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find("div", {"id": "review-body"}).find("div", {"class": "makers"}).find_all("a"):
            vendor_url = f"{BASE_URL}/{a['href']}"
            device_page_urls.append(vendor_url)

    return device_page_urls


def get_crawl_target_urls(token: str, df_already_crawled_html: pd.DataFrame, limit: int = REQUEST_LIMIT) -> pd.DataFrame:
    """
    Get the URLs to crawl for device information.
    Args:
        token (str): The authentication token for the proxy server.
        limit (int): The maximum number of URLs to return.
    Returns:
        pd.DataFrame: A DataFrame containing the URLs to crawl.
    """
    target_vendor_root_urls = get_vendor_root_urls(token)
    target_vendor_urls = get_vendor_urls(target_vendor_root_urls, token)
    target_device_page_urls = get_device_page_urls(target_vendor_urls, token)

    df_target_device_page_urls = pd.DataFrame(target_device_page_urls, columns=["url"])
    df_crawl_target_urls = df_target_device_page_urls[~df_target_device_page_urls["url"].isin(df_already_crawled_html["url"])].head(limit)

    return df_crawl_target_urls


def crawl_from_urls(df: pd.DataFrame, token: str, partition_size: int = 10) -> pd.DataFrame:
    partitioned_list = [df.iloc[i:i+partition_size] for i in range(0, len(df), partition_size)] if not df.empty else [df]
    rows = list()

    for partitioned_df in partitioned_list:
        urls = partitioned_df['url'].tolist()
        for url in urls:
            res = get_request_through_proxy(url, token, headers=HEADERS)

            if res.status_code == 200:
                rows.append({'url': url, 'html': res.text})
            else:
                print(f'Crawling from {url} is failed. Status code: {res.status_code}')

    return pd.DataFrame(rows)


def main(token: str):
    try:
        df_already_crawled_html = pd.read_csv(FILE_PATH)
    except FileNotFoundError:
        df_already_crawled_html = pd.DataFrame(columns=["url"])

    df_crawl_target_urls = get_crawl_target_urls(token, df_already_crawled_html, REQUEST_LIMIT)
    df_crawled_html = crawl_from_urls(df_crawl_target_urls, token)

    pd.concat([df_already_crawled_html, df_crawled_html]).to_csv(FILE_PATH, index=False)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 run.py <proxy token>")
        sys.exit(1)
    proxy_token = sys.argv[1]
    main(proxy_token)
