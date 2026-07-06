from src.common.data_loader import group_rows_by_sample

def test_group_rows_by_sample():
    rows = [
        {"sample_uid":"s1","tag_id":"31"},
        {"sample_uid":"s1","tag_id":"32"},
        {"sample_uid":"s2","tag_id":"40"},
    ]
    g = group_rows_by_sample(rows)
    assert len(g["s1"]) == 2
    assert len(g["s2"]) == 1
