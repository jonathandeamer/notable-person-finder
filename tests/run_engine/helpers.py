from pathlib import Path

OPERATIONAL_SECTIONS = """\
[transport]
contact_url = "https://example.com/contact"

[retry]
max_attempts = 3

[concurrency]
http_workers = 4

[budget]
openrouter_usd_per_run = "2.50"
"""


def write_graph(root: Path, *, operational: str = OPERATIONAL_SECTIONS) -> Path:
    config_file = root / "notable.toml"
    config_file.write_text(
        """\
schema_version = 1
timezone = "Europe/Paris"
feeds_file = "feeds.toml"
domain_profile_file = "profiles/art.toml"

[paths]
root = "portable"

[secrets]
openrouter_api_key = "TEST_OPENROUTER"
brave_api_key = "TEST_BRAVE"
"""
        + operational,
        encoding="utf-8",
    )
    (root / "profiles").mkdir()
    (root / "feeds.toml").write_text(
        """\
schema_version = 1
[[feeds]]
key = "art-news"
label = "Art News"
url = "https://example.com/feed.xml"
enabled = false
""",
        encoding="utf-8",
    )
    (root / "profiles" / "art.toml").write_text(
        """\
schema_version = 1
key = "visual-arts-en"
label = "English visual arts"
language = "en"
[attention_examples]
significant_recognition = ["major art prize"]
""",
        encoding="utf-8",
    )
    return config_file


ENVIRONMENT = {"TEST_OPENROUTER": "or-secret-value", "TEST_BRAVE": "brave-secret-value"}
