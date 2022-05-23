from typing import Optional

from bs4 import BeautifulSoup
from threading import Thread
import requests
from urllib.parse import urlparse, urljoin, ParseResult


class PageFetcher(Thread):

    def __init__(self, obj_scheduler):
        super().__init__()
        self.obj_scheduler = obj_scheduler

    def request_url(self, obj_url: ParseResult) -> Optional[bytes] or None:
        """
        :param obj_url: Instância da classe ParseResult com a URL a ser requisitada.
        :return: Conteúdo em binário da URL passada como parâmetro, ou None se o conteúdo não for HTML
        """
        headers = {
            'User-Agent':
            'CEFET-MG-RI-BOT/1.0 (https://uttermost-card-8b9.notion.site/About-CEFET-MG-RI-BOT-1-0-49c8e0792ba74010b3207c662944d793)',
        }

        response = requests.get(obj_url.geturl(), headers=headers)

        if response.headers['content-type'].__contains__('text/html'):
            return response.content

        return None

    def discover_links(self, obj_url: ParseResult, depth: int,
                       bin_str_content: bytes):
        """
        Retorna os links do conteúdo bin_str_content da página já requisitada obj_url
        """
        soup = BeautifulSoup(bin_str_content, features="lxml")
        for link in soup.select('a'):
            if link.get('href') is not None:
                if link.get('href').startswith('http'):
                    url_link = link.get('href')
                else:
                    url_link = urljoin(obj_url.geturl(), link.get('href'))

                obj_new_url = urlparse(url_link)

                if obj_url.netloc in url_link:
                    new_depth = depth + 1
                else:
                    new_depth = 0

                yield obj_new_url, new_depth

    def crawl_new_url(self):
        """
        Coleta uma nova URL, obtendo-a do escalonador
        """
        next_url, depth = self.obj_scheduler.get_next_url()

        if next_url is not None:
            url_result = self.request_url(next_url)
        else:
            url_result = None

        if url_result is not None:
            discover_links = self.discover_links(next_url, depth, url_result)

            for link_url, link_depth in discover_links:
                if link_url is not None:
                    self.obj_scheduler.add_new_page(link_url, link_depth)

    def run(self):
        """
        Executa coleta enquanto houver páginas a serem coletadas
        """
        while not self.obj_scheduler.has_finished_crawl():
            self.crawl_new_url()
