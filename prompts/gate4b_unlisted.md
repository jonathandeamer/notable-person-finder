# LLM Gate 4b Unlisted — Coverage Verification (Full Source Set)
<!-- prompt-version: 1 -->

You are performing a second-pass check in a structured Wikipedia notability pipeline.

The subject was NOT confirmed as notable using Wikipedia's curated list of reliable sources.
You are now evaluating a broader set of results that includes sources NOT on that curated list.

For each news article listed below, determine TWO things independently:

1. **Is this article genuinely about the named subject?** (`about_subject`)
2. **Is this source a Wikipedia-reliable outlet?** (`is_reliable_source`)

Use ONLY the provided title, description, source domain, and URL. No outside knowledge.

---

## Decision Rules

### `about_subject`
- `true` — The article is primarily about this specific person. Their name is central to the
  headline or description, and the content is clearly focused on them as an individual.
- `false` — A different person with the same name; passing mention; broader story where this
  person is one of many; or ambiguous name match.

### `is_reliable_source`
- `true` — Independently published, professionally edited outlet with editorial standards:
  established newspapers, wire services (AP, Reuters, AFP), established magazines, public
  broadcasters, or academic/scientific publishers.
- `false` — Press release wires (PR Newswire, Business Wire, Globe Newswire, EIN Presswire,
  PRWeb), blogs, personal websites, company/organisation self-promotion, social media,
  aggregators with no editorial oversight, or any site whose editorial independence cannot
  be determined from the domain alone.

A result counts toward notability only if **both** `about_subject` and `is_reliable_source`
are `true`.

---

## Guard Against

- **Namesakes**: If the title or description does not clearly identify which person, return
  `about_subject: false`.
- **Passing mentions**: An article about an event may mention the subject without being
  primarily about them.
- **PR wire content**: prnewswire.com, businesswire.com, globenewswire.com, einpresswire.com
  and similar are NOT editorially independent → `is_reliable_source: false`.
- **Promotional/institutional sites**: A university or charity publishing about someone they
  employ is not an independent source.
- **Default to false** for both fields when uncertain — false negatives are preferable to
  inflating the notability score.

---

## Output Format (STRICT JSON ONLY)

```json
{
  "results": [
    {
      "rank": 1,
      "about_subject": true,
      "is_reliable_source": true,
      "confidence": 0.85,
      "reasoning": "One sentence grounded in the article title, description, or domain."
    }
  ]
}
```

Rules:
- One entry per result, same order as presented, using the original `rank` value
- `about_subject` and `is_reliable_source` must be booleans (`true` or `false`)
- `confidence` is a float 0.0–1.0
- `reasoning` is exactly one sentence grounded in the provided data
- No text outside the JSON object

---

## Input

