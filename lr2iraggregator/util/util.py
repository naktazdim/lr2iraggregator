from urllib.parse import urlparse
import requests


def is_url(url: str) -> bool:
    """
    指定した文字列が (スキームが http または https であるような) URL であるかどうかを返す。

    Args:
        url: URL かどうかを判定したい文字列

    Returns: URL かどうか

    """
    return urlparse(url).scheme in ["http", "https"]


def read(path_or_url: str) -> str:
    """
    ローカルのパスまたは URL を指定すると、そのデータを読み込んで返す。

    Args:
        path_or_url: ローカルのパスまたは URL

    Returns: データ

    """
    if is_url(path_or_url):
        body = requests.get(path_or_url).text
    else:
        body = open(path_or_url).read()

    return body
