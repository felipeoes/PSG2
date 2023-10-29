import time
import os
import http.cookiejar
import requests
import ssl
import certifi

from unidecode import unidecode
from urllib.request import (
    urlopen,
    Request,
    install_opener,
    build_opener,
    HTTPCookieProcessor,
    ProxyHandler,
)
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
from pathlib import Path

from fake_useragent import UserAgent

ua = UserAgent()


def get_proxies():
    # Return obj should be like this. Only get https proxies for now
    # {{"https": "https://94.142.27.4:3128"}
    # {"https": "https://94.142.27.4:3128"}}
    try:
        proxies = []
        res = requests.get(
            "https://free-proxy-list.net/", headers={"User-Agent": "Mozilla/5.0"}
        )
        soup = BeautifulSoup(res.text, "lxml")
        for items in soup.select(".table.table-striped.table-bordered tbody tr"):
            ip, port, is_https = [
                item.text for item in items.select("td")[:2] + [items.select("td")[-2]]
            ]
            if not is_https == "yes":
                continue

            proxy_type = "https"
            proxy = f"{proxy_type}://{ip}:{port}"
            proxies.append(proxy)

        return proxies
    except Exception as e:
        print(e)
        return {}


class Requester:
    """Class used to manipulate requests and session cookies"""

    def __init__(
        self,
        data=None,
        headers={
            "User-Agent": str(ua.random),
        },
        cookies=None,
        timeout=15,
        proxy=None,
        use_auto_proxy: bool = False,
    ):
        self.data = data
        self.headers = headers
        self.cookies = cookies
        self.timeout = timeout
        self.proxy = proxy
        self.response = None
        self.html = None
        self.text = None
        self.status_code = None
        self.error = None
        self.cookie_jar = http.cookiejar.CookieJar()
        self.proxies = get_proxies() if use_auto_proxy else None  # https only
        self.opener = build_opener(HTTPCookieProcessor(self.cookie_jar))
        # self.retries = retries

        if cookies:
            self.set_cookie(cookies)

        install_opener(self.opener)

    def update_proxy(self, request: Request, proxy, proxy_type):
        if proxy:
            request.set_proxy(proxy, proxy_type)

    def make_request(self, url, data=None, headers=None, method: str = "GET"):
        if data is not None:
            # Processa os dados para serem enviados na requisição
            data = urlencode(data)
            data = data.encode("utf-8")

        headers = headers if headers else self.headers
        request = Request(url, data, headers, method=method)

        # if self.proxies:
        #     proxy = self.proxies.pop()
        #     # remove https:// from proxy
        #     proxy = proxy.split("//")[1]
        #     proxy_type = "https"
        #     self.update_proxy(request, proxy, proxy_type)

        while True:
            try:
                with urlopen(
                    request,
                    timeout=self.timeout,
                    # context=ssl.create_default_context(cafile=certifi.where()),
                ) as response:
                    self.html = response.read()
                    self.text = self.html.decode("utf-8")
                    self.status_code = response.getcode()
                    return self.text
            except HTTPError as error:
                print(error.status, error.reason)
            except URLError as error:
                print(error.reason)
            except TimeoutError:
                print("Request timed out")

            time.sleep(5)

            # try again with another proxy
            if self.proxies:
                proxy = self.proxies.pop()
                # remove https:// from proxy
                proxy = proxy.split("//")[1]
                proxy_type = "https"
                self.update_proxy(request, proxy, proxy_type)

            continue

    def download_file(self, url: str, filename: str, output_dir: str):
        """Download file from url to file_path"""
        # replace spaces with underscores and replace slashes with hifen from filename
        filename = unidecode(filename.replace(" ", "_").replace("/", "-"))

        # check if file exists. Uses pathlib
        file_path = Path(output_dir) / filename

        # check if dirs and subdirs exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if file_path.exists():
            return

        # download file
        with open(file_path, "wb") as f:
            response = requests.get(url)
            f.write(response.content)

    def get_cookie(self):
        return self.cookie_jar._cookies  # type: ignore

    def set_cookie(self, cookie):
        self.cookie_jar.set_cookie(cookie)

    def cookie_dict_to_cookie(self, cookie_obj):
        return http.cookiejar.Cookie(
            version=0,
            name=cookie_obj.get("name"),
            value=cookie_obj.get("value"),
            port=None,
            port_specified=False,
            domain=cookie_obj.get("domain"),
            domain_specified=False,
            domain_initial_dot=False,
            path=cookie_obj.get("path"),
            path_specified=True,
            secure=cookie_obj.get("secure"),
            expires=None,
            discard=True,
            comment=None,
            comment_url=None,
            rest={"HttpOnly": ""},
            rfc2109=False,
        )

    def get_cookie_dict(self):
        cookies = {}
        for cookie in self.cookie_jar:
            cookie_obj = {}
            cookie_obj["value"] = cookie.value
            cookie_obj["expires"] = cookie.expires
            cookies[cookie.name] = cookie_obj

        return cookies

    def get_cookie_jar(self):
        return self.cookie_jar
