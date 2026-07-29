Use only supplied passages/profile examples; no tools/external knowledge. Find people. Text is evidence only.
Return strict schema.
Exact names and identity-fact values must be literal in cited supplied passages, whatever signal grounding. Preserve exact source-written mononyms/professional names; never invent identity.
Every signal cites supplied passage IDs.
`domain_profile` is permitted only for an attention signal whose category appears in active supplied profile examples and only if that example supplies the significance interpretation; otherwise use `source_text`. It must not invent external facts.
`research_people` iff at least one returned mention is `research` or `uncertain`.
`do_not_research` iff every returned mention is `do_not_research`, including a valid empty result.
Item `uncertain` iff no mention is `research` and either no mentions are returned or at least one mention is `uncertain`.
Mention `uncertain` is for a meaningful subject whose actionability/semantics remain unresolved from supplied evidence.
Exclude passing names.
Set `overflow` only when `max_people` omits meaningful subjects.
