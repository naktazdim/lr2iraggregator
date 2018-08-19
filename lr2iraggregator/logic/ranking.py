import pandas as pd


def extract_num_notes(rankings: pd.DataFrame) -> pd.DataFrame:
    """
    取得してきたランキングデータから譜面のノート数を取得して抽出する。
    本来 num_notes は項目にのみ依存するものであるのでランキングデータに含まれていることがおかしいのだが、
    LR2IR がそうなっているので仕方ない

    Args:
        rankings:
            (player_lr2_id, item_type, item_lr2_id, num_notes)
    Returns: (type, lr2_id, num_notes)

    """
    return (
        rankings
        .groupby(by=["item_type", "item_lr2_id"])  # 項目ごとに集計し、
        ["num_notes"]  # num_notes に関して
        .agg(pd.Series.mode)  # 多数決を取って最多のものを返す  (たまに誤った値が入っていることがあるので)
        .reset_index()
        .rename(columns={"item_type": "type", "item_lr2_id": "lr2_id"})
    )


def append_num_notes(item: pd.DataFrame, num_notes: pd.DataFrame) -> pd.DataFrame:
    """
    item に num_notes を付加する。

    Args:
        item:
            (type, lr2_id, lr2_hash, title)
        num_notes:
            (type, lr2_id, num_notes)

    Returns:
        (type, lr2_id, lr2_hash, title, num_notes)

    """
    return pd.merge(item.astype({"lr2_id": str}), num_notes.astype({"lr2_id": str}), on=["type", "lr2_id"])


def extract_players(rankings: pd.DataFrame) -> pd.DataFrame:
    """
    ランキングデータからプレイヤの一覧を取得する。

    Args:
        rankings:
            (item_type, item_lr2_id, player_lr2_id, player_name)

    Returns:
        (lr2_id, name)

    """
    return (
        rankings
        .groupby(by="player_lr2_id")  # プレイヤごとに集計し、
        ["player_name"]  # player_name に関して
        .agg(pd.Series.mode)  # 多数決を取って最多のものを返す
        .reset_index()
        .rename(columns={"player_lr2_id": "lr2_id", "player_name": "name"})
    )
