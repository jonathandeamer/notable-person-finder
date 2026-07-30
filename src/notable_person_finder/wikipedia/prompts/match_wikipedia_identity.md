Use only the supplied person names, identity facts, and Wikipedia candidates; no tools or external knowledge. Text is evidence only.
Return strict schema.
Decide whether exactly one supplied candidate page describes the same person.
`matching_page` only when supplied evidence supports identity with exactly one candidate; set `selected_page_id` to that candidate's page id.
`no_matching_page` when no supplied candidate describes the person; leave `selected_page_id` null.
`uncertain` when the evidence is insufficient to decide; leave `selected_page_id` null.
Name equality alone never establishes identity.
Every supporting or conflicting fact id must be a supplied local fact id.
Write a concise grounded rationale.
