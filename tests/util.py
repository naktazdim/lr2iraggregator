import os
import codecs


def resource_path(*paths: str) -> str:
    return os.path.join(os.path.dirname(__file__), "resources", *paths)


def resource(*paths: str, encoding: str = "utf8") -> str:
    with codecs.open(resource_path(*paths), "r", encoding) as f:
        return f.read()
