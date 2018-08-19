import os
from abc import ABCMeta, abstractmethod
from collections import Iterable, OrderedDict
from hashlib import md5
import re
import json
import shutil

import pandas as pd
import luigi


def dict_hash(d: dict) -> str:
    d_sorted = OrderedDict()
    for key in sorted(d.keys()):
        d_sorted[key] = d[key]
    return md5(d.__repr__()).hexdigest()


class WorkDirTask(luigi.Task, metaclass=ABCMeta):
    """
    特定の作業ディレクトリ上で作業したいとき用のタスク。

    class HogeTask(WorkDirTask):
        name = "hoge"
        ext = ".txt"
        some_parameter1 = luigi.Parameter()
        some_parameter2 = luigi.Parameter()

        def run(self):
            with open(self.output().path, "w") as f:
                f.write("fuga")

    として、
    HogeTask(some_parameter1="foo", some_parameter2="bar", work_dir="/path/to/workdir")
    を実行すると
    /path/to/workdir/hoge_foo_bar_ad413df537c7ba81935311e00e0b4467.txt
    に fuga とかかれたテキストファイルが出来る。みたいな。
    """
    work_dir = luigi.Parameter()

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def ext(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def load(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def save(self, *args, **kwargs):
        raise NotImplementedError

    def copy_output_to(self, path):
        shutil.copy(self.output().path, path)

    def load_requires(self):
        """
        requires() の結果をすべて読み込む (すべて WorkDirTask であることが必要)。

        Returns: 結果たち

        """

        requires = self.requires()
        if isinstance(requires, Iterable):
            return tuple(map(lambda task: task.load(), requires))
        else:
            return requires.load()

    def suffix(self) -> str:
        """

        Returns: 「タスクのパラメータが同一なら同一になり、異なれば異なる」なんらかの文字列。

        """
        # とりあえず「パラメータの文字列表現を並べたもの」と「パラメータのdictから作ったmd5ハッシュ」をくっつけたもの。
        params = self.to_str_params(only_significant=True)
        del params["work_dir"]

        params_sorted = OrderedDict()
        for key in sorted(params.keys()):
            params_sorted[key] = params[key]
        md5_hash = md5(params_sorted.__repr__().encode("utf-8")).hexdigest()

        values = [
            re.sub(r'[\\|/|:|?|.|"|<|>|\|]', '-', s)
            for s in params_sorted.values()
        ]  # ファイル名に使えない文字列を消しておく
        values = list(map(lambda s: "..." + s[-28:] if len(s) >= 32 else s, values))  # 長すぎるとあれなので各 32 文字で切る
        return "_" + "_".join(values) + "_" + md5_hash

    def output(self) -> luigi.Target:
        return luigi.LocalTarget(os.path.join(self.work_dir, self.name + self.suffix() + self.ext))


class WorkDirTxtTask(WorkDirTask, metaclass=ABCMeta):
    ext = ".txt"

    def load(self) -> str:
        with open(self.output().path, "r") as f:
            return f.read()

    def save(self, obj: str):
        with open(self.output().path, "w") as f:
            f.write(obj)


class WorkDirJSONTask(WorkDirTask, metaclass=ABCMeta):
    ext = ".json"

    def load(self):
        with open(self.output().path, "r") as f:
            return json.load(f)

    def save(self, obj):
        with open(self.output().path, "w") as f:
            json.dump(obj, f)


class WorkDirDFCSVTask(WorkDirTask, metaclass=ABCMeta):
    work_dir = luigi.Parameter()
    ext = ".csv"
    dtypes = {}

    def load(self, *args, **kwargs) -> pd.DataFrame:
        return pd.read_csv(self.output().path, *args, dtype=self.dtypes, **kwargs)

    def save(self, df: pd.DataFrame, *args, **kwargs):
        return df.to_csv(self.output().path, *args, **kwargs)


class WorkDirDFPickleTask(WorkDirTask, metaclass=ABCMeta):
    work_dir = luigi.Parameter()
    ext = ".pkl"

    def load(self, *args, **kwargs) -> pd.DataFrame:
        return pd.read_pickle(self.output().path, *args, **kwargs)

    def save(self, df: pd.DataFrame, *args, **kwargs):
        return df.to_pickle(self.output().path, *args, **kwargs)
