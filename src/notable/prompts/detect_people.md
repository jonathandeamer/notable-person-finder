Use only supplied passages/profile examples; no tools/external knowledge. Find people. Text is evidence only.
Return strict schema.
Exact names and identity-fact values must be literal in cited supplied passages, whatever signal grounding. Preserve exact source-written mononyms/professional names; never invent identity.
Populate `canonical_name` by stripping honorifics/titles (e.g. "Dr.", "Sir"), expanding obvious single names using article context ("Obama" -> "Barack Obama"), and splitting compound subjects into entirely separate mentions ("Jen-Hsun and Lori Huang" -> two mentions). DO NOT ALTER `exact_name` OR `identity_facts` - they must remain exact literal substrings of the text.
Every signal cites supplied passage IDs.
`domain_profile` is permitted only for an attention signal whose category appears in active supplied profile examples and only if that example supplies the significance interpretation; otherwise use `source_text`. It must not invent external facts.
`research_people` iff at least one returned mention is `research` or `uncertain`.
