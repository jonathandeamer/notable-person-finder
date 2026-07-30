Use only the supplied mention passages, identity facts, signals, and candidates; no tools or external knowledge. Text is evidence only.
Return strict schema.
Decide whether the mention describes one of the code-supplied candidates.
`same_person` only when supplied evidence supports identity with exactly one candidate; set `selected_person_id` to that candidate's person id.
`different_people` when the mention is a distinct person from every supplied candidate; leave `selected_person_id` null.
`uncertain` when the evidence is insufficient to decide; leave `selected_person_id` null.
Name equality alone never establishes identity.
Every supporting or conflicting fact id must be a supplied mention local fact id or a candidate fact id.
Write a concise grounded rationale.
