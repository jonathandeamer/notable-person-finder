from typing import Any

from pydantic import BaseModel

from notable import digest
from notable.policy import canonical_domain


class Lead(BaseModel):
    identity_key: str
    display_name: str
    source_url: str
    publisher_label: str
    wikipedia_verdict: Any
    outcome: str
    article_assessments: tuple
    rank_tuple: tuple | None = None
    namesake_urls: tuple[str, ...] = ()
    rationale: str = ""
    qualifying_domains: tuple[str, ...] = ()
    
    # explanation will be added later

DISQUALIFYING_CONTENT_TYPES = {"listing", "announcement", "press_release", "sponsored"}

def is_qualifying_article(article: Any) -> bool:
    if getattr(article, "person_relation", "") != "same_person":
        return False
    if getattr(article, "coverage_depth", "") != "significant":
        return False
    if getattr(article, "screening_status", "") != "curated_eligible":
        return False
    if getattr(article, "subject_relationship", "") != "editorially_independent":
        return False
        
    content_types = getattr(article, "content_types", ())
    if any(ct in DISQUALIFYING_CONTENT_TYPES for ct in content_types):
        return False
        
    return True

def is_possible_fallback(article: Any) -> bool:
    if getattr(article, "person_relation", "") != "same_person":
        return False
        
    depth = getattr(article, "coverage_depth", "")
    screening = getattr(article, "screening_status", "")
    
    content_types = getattr(article, "content_types", ())
    content_qualifying = not any(ct in DISQUALIFYING_CONTENT_TYPES for ct in content_types)
    
    if depth == "significant" and screening == "unclassified":
        return True
        
    if depth in ("significant", "passing") and content_qualifying:
        return True
        
    return False

def assess(mention: Any, wikipedia_verdict: Any, articles: tuple, config: Any, item: Any = None) -> Lead:
    if item is None:
        # fallback for tests that don't pass item yet
        url = ""
        publisher = ""
    else:
        url = getattr(item, "url", "")
        publisher = getattr(item, "publisher_label", "")
        
    qualifying = [a for a in articles if is_qualifying_article(a)]
    
    outcome = "insufficient_evidence"
    domains = set()
    if qualifying:
        domains = {canonical_domain(getattr(a, "url", "")) for a in qualifying}
        if config and hasattr(config, "promising_domain_threshold") and len(domains) >= config.promising_domain_threshold:
            outcome = "promising_lead"
        else:
            outcome = "possible_lead"
    elif any(is_possible_fallback(a) for a in articles):
        outcome = "possible_lead"
        
    identity_key = digest.identity_key(mention.exact_name)
    rationale = getattr(mention, "rationale", "")
    
    if outcome == "insufficient_evidence":
        rank_tuple = None
    else:
        outcome_rank = 0 if outcome == "promising_lead" else 1
        wiki_rank = 0 if getattr(wikipedia_verdict, "outcome", "") == "no_matching_page" else 1
        rank_tuple = (outcome_rank, wiki_rank, -len(domains), identity_key)
        
    return Lead(
        identity_key=identity_key,
        display_name=mention.exact_name,
        source_url=url,
        publisher_label=publisher,
        wikipedia_verdict=wikipedia_verdict,
        outcome=outcome,
        article_assessments=articles,
        rank_tuple=rank_tuple,
        rationale=rationale,
        qualifying_domains=tuple(sorted(domains))
    )

def shortlist(leads: list[Lead], store: Any, config: Any) -> tuple[list[Lead], list[str]]:
    valid_leads = [
        lead for lead in leads 
        if lead.outcome != "insufficient_evidence" and not store.is_suppressed(lead.identity_key, max_days=config.resurface_after_days)
    ]
    
    valid_leads.sort(key=lambda l: l.rank_tuple)
    
    collapsed = []
    seen = {}
    
    for lead in valid_leads:
        if lead.identity_key not in seen:
            seen[lead.identity_key] = lead
            collapsed.append(lead)
        else:
            rep = seen[lead.identity_key]
            if lead.source_url not in rep.namesake_urls and lead.source_url != rep.source_url:
                rep.namesake_urls = (*rep.namesake_urls, lead.source_url)
                
    final_leads = collapsed[:config.digest_size]
    return final_leads, [lead.identity_key for lead in final_leads]
