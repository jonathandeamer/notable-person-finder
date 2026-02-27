import unittest

from name_utils import normalize_name


class TestNameUtils(unittest.TestCase):
    def test_normalize_basic(self) -> None:
        self.assertEqual(normalize_name("Donald Trump"), "donald trump")

    def test_normalize_parens(self) -> None:
        self.assertEqual(normalize_name("John Smith (politician)"), "john smith")

    def test_normalize_diacritics(self) -> None:
        self.assertEqual(normalize_name("José Ángel"), "jose angel")


if __name__ == "__main__":
    unittest.main()
