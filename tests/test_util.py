# -*- coding: utf-8 -*-
import unittest

from lr2iraggregator.util.util import *


class TestUtilLogics(unittest.TestCase):
    def test_is_url(self):
        paths = [
            "https://nekokan.dyndns.info/~lobsak/genocide/insane.html",
            "http://achusi.main.jp/overjoy/nanido-luna.php",
            "/Users/Example/",
            "foo",
            "./bar",
            r"C:\Documents and Settings\Example"
        ]
        answers = [True, True, False, False, False, False]

        for path, answer in zip(paths, answers):
            with self.subTest(path):
                self.assertEqual(is_url(path), answer)


if __name__ == '__main__':
    unittest.main()
