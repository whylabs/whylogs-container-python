from .string_util import encode_strings


def test_order_doesnt_matter() -> None:
    column_names_1 = ["col1", "col2", "col4"]
    column_names_2 = ["col4", "col1", "col2"]

    hash1 = encode_strings(column_names_1)
    hash2 = encode_strings(column_names_2)

    assert hash1 == hash2


def test_different_hashes() -> None:
    column_names_1 = ["col1", "col2", "col4"]
    column_names_2 = ["col1", "col2", "col3"]

    hash1 = encode_strings(column_names_1)
    hash2 = encode_strings(column_names_2)

    assert hash1 != hash2
