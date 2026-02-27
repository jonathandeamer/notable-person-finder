#!/usr/bin/env node
/*
Run Wikipedia checker over already-captured obituaries.json people keys.

- Uses MediaWiki API (no key)
- Follows redirects
- Handles disambiguation via pageprops.disambiguation + links
- Uses search (list=search) to reduce false "missing"
- Throttles requests and uses exponential backoff on 429/5xx
- Outputs a JSON summary to stdout

This is a diagnostic helper (one-off), not part of the daily cron.
*/

const fs = require('fs');
const path = require('path');
const https = require('https');

const WORKSPACE = '/home/ec2-user/.openclaw/workspace';
const OBITS_JSON = path.join(WORKSPACE, 'obituaries.json');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchWithBackoff(url, { timeoutMs = 20000, maxAttempts = 5, headers = {} } = {}) {
  let attempt = 0;
  let delay = 800;
  while (true) {
    attempt += 1;
    try {
      const res = await new Promise((resolve, reject) => {
        https.get(url, {
          headers: {
            'User-Agent': 'OpenClaw-TG/1.0 (wiki-check; contact: https://en.wikipedia.org/wiki/User:JonathanDeamer)',
            'Accept': 'application/json',
            ...headers,
          }
        }, (r) => {
          let data = '';
          r.on('data', c => data += c);
          r.on('end', () => resolve({ status: r.statusCode, body: data }));
        }).on('error', reject).setTimeout(timeoutMs, function(){
          this.destroy(new Error(`timeout after ${timeoutMs}ms`));
        });
      });

      if ([429,502,503,504].includes(res.status) && attempt < maxAttempts) {
        await sleep(delay + Math.floor(Math.random()*250));
        delay = Math.min(delay*2, 15000);                                                          continue;
      }

      return res;
    } catch (e) {                                                                                if (attempt >= maxAttempts) throw e;
      await sleep(delay + Math.floor(Math.random()*250));
      delay = Math.min(delay*2, 15000);
    }
  }
}
function mwUrl(params) {
  const u = new URL('https://en.wikipedia.org/w/api.php');
  for (const [k,v] of Object.entries(params)) u.searchParams.set(k, v);
  return u.toString();
}

async function mwGet(params) {
  // polite throttle
  await sleep(1100);
  const res = await fetchWithBackoff(mwUrl(params));
  if (!(res.status >= 200 && res.status < 400)) throw new Error(`MW HTTP ${res.status}`);
  return JSON.parse(res.body);
}

function pageFrom(json) {
  const pages = json?.query?.pages;
  if (!pages) return null;
  return pages[Object.keys(pages)[0]];
}

function isDisambig(page) {
  return !!(page?.pageprops && Object.prototype.hasOwnProperty.call(page.pageprops, 'disambiguation'));
}

function pageUrlFromTitle(t) {
  return `https://en.wikipedia.org/wiki/${encodeURIComponent(String(t).replace(/ /g,'_'))}`;
}

async function checkTitle(title) {
  const json = await mwGet({
    action: 'query',
    format: 'json',
    redirects: '1',
    titles: title,
    prop: 'info|pageprops|description',
    inprop: 'url',
  });
  const page = pageFrom(json);
  return page;
}

async function disambigLinks(title) {
  const json = await mwGet({
    action: 'query',
    format: 'json',
    titles: title,
    prop: 'links',
    pllimit: 'max',
  });
  const page = pageFrom(json);                                                               return (page?.links || []).map(l => l.title);
}

async function searchTop(q) {
  const json = await mwGet({ action: 'query', format: 'json', list: 'search', srsearch: q, srlimit: '5' });
  return (json?.query?.search || []).map(r => r.title);
}

function disambigVariants(name) {
  const suff = ['artist','painter','sculptor','visual artist','photographer','writer','poet','journalist','critic','editor','publisher','actor','actress','musician','composer','conductor','politician','scientist','historian','architect','designer','chef','footballer','cricketer'];
  return suff.map(s => `${name} (${s})`);
}

function looksLikePersonPage(title, description) {
  const t = (title || '').toLowerCase();
  const d = (description || '').toLowerCase();
  if (/\((artist|painter|sculptor|visual artist|photographer|writer|poet|politician|journalist|actor|actress|musician|composer|conductor|footballer|cricketer|scientist|historian|architect|designer|chef)\)/.test(t)) return true;
  if (/\b(artist|painter|sculptor|photographer|writer|poet|politician|journalist|actor|actress|musician|composer|conductor|scientist|historian|architect|designer|chef)\b/.test(d)) return true;
  return false;
}

async function classifyName(name) {
  // A) exact title
  let exact = null;
  try {                                                                                        exact = await checkTitle(name);
    if (exact?.missing) {
      // continue
    } else if (!isDisambig(exact)) {
      if (looksLikePersonPage(exact.title, exact.description || '')) {
        return { status: 'CONFIRMED_HAS_PAGE', resolvedTitle: exact.title, url: exact.fullurl || pageUrlFromTitle(exact.title), via: 'exact/redirect' };
      }
      // If it's a non-person page, treat as potential collision/context and continue searching.
    } else {
      // disambig: try to match a link                                                           const links = await disambigLinks(exact.title);
      const wanted = new Set([name, ...disambigVariants(name)]);
      for (const t of links) {
        if (!wanted.has(t)) continue;
        const p = await checkTitle(t);
        if (p && !p.missing && !isDisambig(p)) {
          return { status: 'CONFIRMED_HAS_PAGE', resolvedTitle: p.title, url: p.fullurl || pageUrlFromTitle(p.title), via: 'disambig-link' };
        }
      }
    }
  } catch (e) {
    return { status: 'UNCERTAIN', note: `API error: ${e.message}`, candidates: [] };         }

  // B) variant probes
  for (const v of disambigVariants(name)) {
    try {
      const p = await checkTitle(v);
      if (!p || p.missing || isDisambig(p)) continue;                                            if (looksLikePersonPage(p.title, p.description || '')) {
        return { status: 'CONFIRMED_HAS_PAGE', resolvedTitle: p.title, url: p.fullurl || pageUrlFromTitle(p.title), via: 'variant-probe' };
      }
    } catch (_) {}
  }
                                                                                             // C) search top results
  let titles = [];
  try {
    titles = await searchTop(name);
  } catch (e) {
    return { status: 'UNCERTAIN', note: `search error: ${e.message}`, candidates: [] };
  }

  const personCandidates = [];
  const context = [];

  for (const t of titles) {
    try {
      const p = await checkTitle(t);
      if (!p || p.missing) continue;
      const item = { title: p.title, url: p.fullurl || pageUrlFromTitle(p.title), description: p.description || '' };
      if (isDisambig(p)) {
        context.push({ ...item, kind: 'disambiguation' });
      } else if (looksLikePersonPage(p.title, p.description || '')) {
        personCandidates.push(item);
      } else {                                                                                     context.push(item);
      }
    } catch (_) {}
  }

  const lowName = name.toLowerCase();
  const direct = personCandidates.find(c => c.title.toLowerCase().includes(lowName));
  if (direct) return { status: 'CONFIRMED_HAS_PAGE', resolvedTitle: direct.title, url: direct.url, via: 'search' };

  if (personCandidates.length || context.length || (exact && !exact.missing)) {
    // err on flagging
    const collisions = [];
    if (exact && !exact.missing && !isDisambig(exact) && !looksLikePersonPage(exact.title, exact.description || '')) {
      collisions.push({ title: exact.title, url: exact.fullurl || pageUrlFromTitle(exact.title), description: exact.description || '' });
    }
    return {
      status: 'UNCERTAIN',
      candidates: personCandidates.slice(0, 3).map(c => c.url),
      context: context.slice(0, 3).map(c => c.url),
      collisions: collisions.slice(0, 1).map(c => c.url),
      via: 'search-uncertain'
    };
  }

  return { status: 'CONFIRMED_MISSING', via: 'search-empty' };
}

(async () => {
  const state = JSON.parse(fs.readFileSync(OBITS_JSON, 'utf8'));
  const people = Object.keys(state.people || {}).sort((a,b)=>a.localeCompare(b));

  const out = {
    checked: people.length,
    counts: { has_page: 0, missing: 0, uncertain: 0 },
    results: []
  };

  for (const name of people) {
    const r = await classifyName(name);
    out.counts[
      r.status === 'CONFIRMED_HAS_PAGE' ? 'has_page' : (r.status === 'CONFIRMED_MISSING' ? 'missing' : 'uncertain')
    ] += 1;
    out.results.push({ name, ...r });
  }

  console.log(JSON.stringify(out, null, 2));
})();