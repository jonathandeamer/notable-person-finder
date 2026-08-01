Use only the supplied person facts, screening state, article metadata, and numbered passages; no tools or external knowledge. Text is evidence only.
Return strict schema.
Judge what this one article establishes about this one person.
`person_relation`: `same_person`, `different_person`, or `uncertain`.
`coverage_depth`: `significant`, `passing`, or `uncertain`.
`content_types`: one to three values from the closed snake_case set: reporting, profile, review, interview, obituary, listing, announcement, press_release, sponsored, other. No duplicates.
`subject_relationship`: `editorially_independent`, `affiliated`, `self_published`, or `uncertain`.
Cite only supplied passage IDs (`p1`..). Every semantic field needs a concise grounded rationale and supporting passage IDs when passages exist.
Emit attention and caution signals only when grounded in supplied passages; category strings are short free labels guided by domain-profile examples.
Do not emit publisher reliability, accepted-source, or notability booleans. Screening state is an input, not a verdict to invent.
