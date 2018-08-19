import pandas as pd


def calculate_dan(record: pd.DataFrame, dan_table: pd.DataFrame) -> pd.DataFrame:
    """
    ランプの記録と段位表からプレイヤの段位を計算する。

    Args:
        record:
            (item_type, item_lr2_id, player_lr2_id, clear)
        dan_table:
            (type, lr2_id, level)
            level は整数値で、段位順に並んでいるものとする。
            例えば初段: 1, 二段: 2, ……, 十段: 10, 皆伝: 11, Overjoy: 12

    Returns:
        (lr2_id, dan)

    """
    return (
        pd.merge(record[record.clear.astype(int) >= 3].astype(str),  # 記録のうち、クリア済みであるものと
                 dan_table.astype(str),  # 段位が記載されたデータとを
                 left_on=["item_type", "item_lr2_id"],
                 right_on=["type", "lr2_id"],
                 how="inner")  # 内部結合して (= 段位に関するデータだけ抜き出して)
        [["player_lr2_id", "level"]]  # 要るカラムだけ取ってきて
        .astype({"level": int})
        .groupby("player_lr2_id")  # プレイヤごとの
        .agg("max")  # (level の) 最大値を取ればそれが段位
        .reset_index()
        .rename(columns={"player_lr2_id": "lr2_id", "level": "dan"})
        .astype({"dan": str})
    )


def calculate_dans(bms_table_list: pd.DataFrame, level: pd.DataFrame, record: pd.DataFrame) -> pd.DataFrame:
    """
    プレイヤ・段位の一覧を作って返す。

    Args:
        bms_table_list: (name, path, is_dan)
        record: (item_type, item_lr2_id, player_lr2_id, clear)
        level: (type, lr2_id, bms_table_name, level)

    Returns:
        (lr2_id, dan_name, dan)
    """
    dans = [pd.DataFrame(columns=["lr2_id", "dan_name", "dan"])]
    for _, row in bms_table_list.iterrows():
        if row["is_dan"]:
            dan_name = row["name"]
            dan_table = (
                level
                [level["bms_table_name"] == dan_name]
                [["type", "lr2_id", "level"]]
                .reset_index(drop=True)
            )
            dans.append(
                calculate_dan(record, dan_table)
                .assign(dan_name=dan_name)
                [["lr2_id", "dan_name", "dan"]]
            )

    return pd.concat(dans).reset_index(drop=True)
