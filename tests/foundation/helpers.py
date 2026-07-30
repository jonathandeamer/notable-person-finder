from pathlib import Path


def write_graph(root: Path) -> Path:
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

[openrouter]
endpoint = "https://openrouter.ai/api/v1"

[openrouter.routing]
allow_fallbacks = true
data_collection = "deny"
zdr = true

[mediawiki]
endpoint = "https://en.wikipedia.org/w/api.php"
maxlag_seconds = 5

[tasks.detect_people]
model = "openai/gpt-5.4-mini"
max_input_tokens = 4096
max_completion_tokens = 1024
max_people = 8
max_title_characters = 500
max_summary_characters = 4000

[tasks.detect_people.parameters]
temperature = 0.0
top_p = 1.0
reasoning_effort = "low"

[tasks.resolve_person_entity]
model = "openai/gpt-5.4-mini"
max_input_tokens = 4096
max_completion_tokens = 1024
max_candidates = 8
max_facts_per_candidate = 12
max_names_per_candidate = 8
max_title_characters = 500
max_summary_characters = 4000

[tasks.resolve_person_entity.parameters]
temperature = 0.0
top_p = 1.0
reasoning_effort = "low"

[tasks.match_wikipedia_identity]
model = "openai/gpt-5.4-mini"
max_input_tokens = 4096
max_completion_tokens = 1024
max_candidates = 8
max_query_forms = 6
search_srlimit = 10
max_continuations_per_form = 1
max_search_hits_per_form = 20
max_page_ids_per_facts_request = 20
max_redirect_hops = 3
max_fact_pages_per_plan = 40
max_extract_characters = 1200
max_categories_per_page = 20
max_names_in_prompt = 8
max_facts_in_prompt = 16
refresh_interval_hours = 720
max_title_characters = 500
max_summary_characters = 4000

[tasks.match_wikipedia_identity.parameters]
temperature = 0.0
top_p = 1.0
reasoning_effort = "low"
""",
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
