# LLM Gate 4b Prompt — Coverage Verification
<!-- prompt-version: 1 -->

You are performing **Gate 4b** in a structured Wikipedia notability pipeline.

Your task:

For each news article listed below, determine whether the article is **genuinely about the named subject** as its **primary subject** — not merely mentioning them, and not about a different person with the same name.

You must use **ONLY the provided article title, description, and source domain** — no outside knowledge.

**Do NOT use outside knowledge.**
**Do NOT assume the subject is the primary focus without clear evidence from the title or description.**
**Never guess.**

---

## Decision Rules

For each result, decide:

- `about_subject: true` — The article is **primarily about this specific person**. Their name is central to the headline or description, and the content is clearly focused on them as an individual.
- `about_subject: false` — The article is NOT primarily about this person. This includes:
  - A different person with the same name
  - A passing mention of the person in a broader story
  - A wider story where this person is one of many subjects
  - Content where the name match is incidental or ambiguous

### Guard against:

- **Namesakes**: "Brian Cox" could be the actor, the physicist, or another person entirely. If the title or description does not clearly identify which person, return `false`.
- **Passing mentions**: An article about an event or organisation may mention the subject without being primarily about them.
- **Unrelated content**: Articles that share a name keyword but are clearly focused on something else.

---

## Strict Constraints

- Use ONLY the title, description, and source domain provided for each result.
- Do NOT use outside knowledge to determine if the article is about this person.
- Do NOT infer the article is about the subject solely from the subject's source context.
- If the title or description is ambiguous or generic, set `about_subject: false`.
- **Default to `false` when uncertain** — a false negative adds to the review queue; a false positive inflates the notability score.

---

## Output Format (STRICT JSON ONLY)

Return exactly the following JSON object and nothing else:

```json
{
  "results": [
    {
      "rank": 1,
      "about_subject": true,
      "confidence": 0.9,
      "reasoning": "One sentence grounded in the article title or description."
    }
  ]
}
```

Rules:
- Return one entry per result, in the same order as presented, using the original `rank` value
- `about_subject` must be a boolean (`true` or `false`)
- `confidence` is a float from 0.0 to 1.0 (e.g. 0.9 for high confidence, 0.4 for uncertain)
- `reasoning` is exactly one sentence grounded in the provided title or description text
- No commentary outside the JSON object

---

## Input

