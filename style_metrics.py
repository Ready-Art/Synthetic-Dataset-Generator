"""Pure-stdlib style / tic metrics.

Ported from the scotoma analysis scripts (comma_rate, tail_positivity, depth_proxies,
coherence_defects, negation_census, narrative_neg, two_tics, denial_correction).
No third-party imports, no network, no project imports: safe to unit test standalone.
"""
import re

# --- one sentence splitter for every metric -----------------------------------
_SENT_SPLIT = re.compile(r'(?<=[.!?\u2026])\s+(?=[^\s])')


def split_sentences(text, min_words=1):
    parts = [s.strip() for s in _SENT_SPLIT.split((text or '').strip()) if s.strip()]
    if min_words <= 1:
        return parts
    return [s for s in parts if len(s.split()) >= min_words]


# --- narration vs dialogue segmentation (narrative_neg.segments) -----------------
_AST_RE = re.compile(r'\*[^*]+\*', re.S)
_QUOTE_RE = re.compile(r'"[^"]*"|\u201c[^\u201d]*\u201d', re.S)


def segments(text):
    text = text or ''
    spans, out, pos = [], [], 0
    if '*' in text:
        spans = [(m.start(), m.end(), 'narr') for m in _AST_RE.finditer(text)]
        other = 'dial'
    elif '"' in text or '\u201c' in text:
        spans = [(m.start(), m.end(), 'dial') for m in _QUOTE_RE.finditer(text)]
        other = 'narr'
    else:
        return [(0, len(text), 'narr')]
    for s, e, k in spans:
        if s > pos:
            out.append((pos, s, other))
        out.append((s, e, k))
        pos = e
    if pos < len(text):
        out.append((pos, len(text), other))
    return out


# --- comma habits (comma_rate.stats) ----------------------------------------
def comma_stats(texts):
    per_resp, all_sents_c = [], []
    commas = words = 0
    for t in texts or ():
        t = (t or '').strip()
        if not t:
            continue
        sents = split_sentences(t)
        if not sents:
            continue
        cs = [s.count(',') for s in sents]
        all_sents_c.extend(cs)
        per_resp.append(sum(cs) / float(len(cs)))
        commas += t.count(',')
        words += len(t.split())
    if not per_resp:
        return {'commas_per_sent': 0.0, 'commas_per_100w': 0.0,
                'sents_3plus_commas_pct': 0.0, 'n': 0}
    return {
        'commas_per_sent': sum(per_resp) / len(per_resp),
        'commas_per_100w': 100.0 * commas / max(words, 1),
        'sents_3plus_commas_pct': 100.0 * sum(1 for c in all_sents_c if c >= 3) / len(all_sents_c),
        'n': len(per_resp),
    }


# --- tail-end positivity (tail_positivity) -----------------------------------
_TAIL_POS = frozenset("""
warm warmth soft softly gentle gently smile smiles smiling promise promises safe content
peace peaceful tender fond affection comfort comfortable close closer glow glowing settles
settled eases easy quiet quietly hope trust
""".split())

_TAIL_SIG = re.compile(
    r"(say|said|says|speak|spoke)s? (everything|volumes|more than)"
    r"|the way (he|she|they|it|you)"
    r"|need(ed)? no (words|answer)"
    r"|didn'?t need (words|to say)"
    r"|(everything|nothing) (else )?(needed|left)"
    r"|hang(s|ing)? in the air"
    r"|linger(s|ing)? (in|between|on)"
    r"|something (unspoken|unsaid|shift(s|ed)|pass(es|ed)) between"
    r"|for now\W*$"
    r"|(night|day|evening) (is|was) (still )?(young|far from over)", re.I)

_TAIL_BUT = re.compile(r", but |, yet ", re.I)


def _rate(sents, fn):
    if not sents:
        return 0.0
    return 100.0 * sum(1 for s in sents if fn(s)) / len(sents)


def tail_positivity(text):
    """Positional lift of warm closers / significance closers over body sentences."""
    sents = split_sentences(text, min_words=3)
    if len(sents) < 3:
        return {'affect_body': 0.0, 'affect_final': 0.0, 'affect_lift': 0.0,
                'sig_body': 0.0, 'sig_final': 0.0, 'sig_lift': 0.0,
                'but_pivot_final': 0.0, 'n': len(sents)}

    def pos_hit(s):
        return bool(set(re.findall(r"[a-z']+", s.lower())) & _TAIL_POS)

    body, final = sents[:-1], sents[-1]
    ab, af = _rate(body, pos_hit), _rate([final], pos_hit)
    sb = _rate(body, lambda s: bool(_TAIL_SIG.search(s)))
    sf = _rate([final], lambda s: bool(_TAIL_SIG.search(s)))
    # Lift is CLAMPED: a body rate of zero makes the raw ratio unbounded (100/0.01 = 10000),
    # which would swamp any mean it is averaged into. 10.0 means "at least a tenfold lift".
    return {
        'affect_body': ab, 'affect_final': af,
        'affect_lift': min(10.0, af / max(ab, 0.1)),
        'sig_body': sb, 'sig_final': sf,
        'sig_lift': min(10.0, sf / max(sb, 0.01)),
        'but_pivot_final': _rate([final], lambda s: bool(_TAIL_BUT.search(s))),
        'n': len(sents),
    }


# --- depth proxies (depth_proxies) ------------------------------------------
_INTERIOR = re.compile(
    r"\b(feel|feels|felt|feeling|notice[sd]?|noticing|realiz\w+|remember\w*|"
    r"wonder\w*|sens\w+|consider\w*|regret\w*|hesitat\w+|ache[sd]?|aching|"
    r"long(s|ed|ing)? for|imagin\w+|recall\w*|dread\w*|savou?r\w*)\b", re.I)
_WORD4 = re.compile(r"[a-z']{4,}")
_DEEP_STOP = frozenset("""
that this with have from they them their there what when your will would could
should about which while been being were says said like just even only
""".split())


def narration_share(text):
    text = text or ''
    inside = sum(len(s) for s in re.findall(r"\*([^*]+)\*", text))
    return 100.0 * inside / max(len(text), 1)


def grounding(resp, context_text, last_user_text=''):
    """% of response content words that also appear in DEEP context (long-range attention)."""
    deep = (context_text or '').lower()
    if not deep:
        return None
    rw = [w for w in _WORD4.findall((resp or '').lower()) if w not in _DEEP_STOP]
    if not rw:
        return None
    last_user = (last_user_text or '').lower()
    hits = sum(1 for w in rw if w in deep and w not in last_user)
    return 100.0 * hits / len(rw)


def depth_proxies(text, context_text='', last_user_text=''):
    text = text or ''
    wc = len(text.split())
    g = grounding(text, context_text, last_user_text)
    return {
        'grounding_per_100w': -1.0 if g is None else g,
        'narration_pct': narration_share(text),
        'interior_per_100w': 100.0 * len(_INTERIOR.findall(text)) / max(wc, 1),
        'words': wc,
    }


# --- coherence defects (coherence_defects.defects) ----------------------------
_SCRIPT_RE = re.compile(u"[\u4e00-\u9fff\u3040-\u30ff\u0400-\u04ff\u0900-\u097f\uac00-\ud7af]")
_FOREIGN_ART = re.compile(
    r"\b(?:is|was|a|the|an)\s+(la|le|les|el|der|die|das|il|lo|un[ae]?)\s+\w", re.I)
_DET = frozenset("""
the a an this that these those his her its their
""".split())
_COH_WORD = re.compile(r"[A-Za-z']+")
_COH_STOP = frozenset("""
the a an and or but of to in on at is was her his its their with for as it he she they you
""".split())
_KAOMOJI = re.compile(u"[（(][^）)]{0,20}[\uff89\u3063\u25d5\u30ee\u02d8\u0329\u256d\u2570\u2727]"
                       u"[^）)]{0,20}[）)]|[\uff89\u3063]\\S{0,6}[\u2727\u30fb\uff9f]")
_SCRIPT_WORD = re.compile(u"[\u4e00-\u9fff\u0400-\u04ff\u0900-\u097f\uac00-\ud7af]")


def has_script_contamination(text):
    """A non-Latin WORD, ignoring decorative kaomoji (mark_corpus_defects)."""
    return bool(_SCRIPT_WORD.search(_KAOMOJI.sub(" ", text or "")))


def coherence_defects(text, echo_enabled=False):
    text = text or ''
    d = {}
    d['script_mix'] = int(bool(_SCRIPT_RE.search(text)))
    d['foreign_article'] = int(bool(_FOREIGN_ART.search(text)))
    w = [x.lower() for x in _COH_WORD.findall(text)]
    echo = 0
    if echo_enabled:
        for i, x in enumerate(w):
            if x in _COH_STOP or len(x) < 4:
                continue
            if x in w[i + 1:i + 4]:
                echo += 1
    d['echo_bigram'] = int(echo > 0)
    d['echo_count'] = echo
    d['det_stutter'] = int(any(w[i] in _DET and w[i + 1] in _DET for i in range(len(w) - 1)))
    sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if len(s.strip()) > 25]
    d['dup_sentence'] = int(len(sents) != len(set(sents)))
    d['any'] = int(d['script_mix'] or d['foreign_article'] or d['echo_bigram']
                    or d['det_stutter'] or d['dup_sentence'])
    return d


# --- negation profile (negation_census + narrative_neg) -----------------------
_NEG_TOKEN = re.compile(
    r"\b(?:is|are|was|were|does|do|did|has|have|had|can|could|will|would|should)n['\u2019]t\b"
    r"|\b(?:not|never|nothing|nobody|no longer|neither|nor|cannot)\b", re.I)
_ELLIPSIS_OPENER = re.compile(
    r"^\s*[\"'\u201c\u2014(*]?\s*(?:Didn'?t|Doesn'?t|Don'?t|Wasn'?t|Isn'?t|Hadn'?t|"
    r"Hasn'?t|Won'?t|Wouldn'?t|Couldn'?t|Can'?t|Never|Not)\b", re.I)
_CONTRA = re.compile(
    r"\b(?:is|are|was|were|does|do|did|has|have|had|can|could|will|would)n['\u2019]t\b[^.;!?]{0,80}"
    r"[;,.\u2014-]\s*(?:it|he|she|they|instead|rather|more of|but)\b"
    r"|\bnot\s+(?:just|only|merely|exactly|so much)\b", re.I)


def negation_profile(text):
    """Narration-only negation: density, consecutive runs, dropped-subject fragments."""
    text = text or ''
    n_narr = n_neg = n_ell = n_con = 0
    runs, max_run = [], 0
    for s, e, kind in segments(text):
        if kind != 'narr':
            continue
        frag = text[s:e]
        if len(frag.strip()) < 3:
            continue
        flags = []
        for sent in split_sentences(frag):
            neg = bool(_NEG_TOKEN.search(sent))
            flags.append(neg)
            if neg:
                n_neg += 1
                if _ELLIPSIS_OPENER.match(sent):
                    n_ell += 1
                if _CONTRA.search(sent):
                    n_con += 1
        n_narr += len(flags)
        cur = 0
        for f in flags:
            if f:
                cur += 1
            elif cur:
                runs.append(cur)
                cur = 0
        if cur:
            runs.append(cur)
    max_run = max(runs) if runs else 0
    return {
        'narr_sent': n_narr,
        'narr_neg': n_neg,
        'neg_per_100_sent': 100.0 * n_neg / max(n_narr, 1),
        'bare_pct': 100.0 * (n_neg - n_con) / max(n_neg, 1),
        'ellipsis_pct': 100.0 * n_ell / max(n_neg, 1),
        'has_run': int(any(r >= 2 for r in runs)),
        'run_ge2': sum(1 for r in runs if r >= 2),
        'max_run': max_run,
    }


# --- denial-then-correction couplet (denial_correction, regex approximation) --
_SCALAR_RE = re.compile(r"\bnot\s+(\w+\s+){0,2}enough\b", re.I)
_META_RE = re.compile(
    r"\b(the user|i should|the model|stay in character|the prompt says|"
    r"my response|the scene requires|HYPOTHESIS|REWARD:)\b", re.I)
_NEG_IN_CLAUSE = re.compile(
    r"\b(?:is|are|was|were|does|do|did|has|have|had|can|could|will|would|should)n['\u2019]t\b"
    r"|\b(?:not|never|nothing|nobody|no longer|neither|nor|cannot)\b", re.I)
# The correction half must be AFFIRMATIVE ("She didn't move. She didn't speak." is a
# negation RUN, not a denial-then-correction), so the tail is checked after the match.
_DENIAL_CORRECTION = re.compile(
    r"\b(?:is|are|was|were|does|do|did|has|have|had|can|could|will|would|should)n['\u2019]t\b"
    r"[^.;!?\n]{0,140}?[;.!?\u2014\u2013-]\s*"
    r"(?:it|he|she|they|this|that|there|instead|rather|if anything)\b[^.!?\n]{0,140}",
    re.I)


def denial_correction_hits(text):
    """Narration-only spans matching 'deny a predicate, then assert the replacement'.

    Regex approximation of the spaCy implementation: no dependency parse, so the
    subject-coreference test is approximated by a pronoun / 'instead' / 'rather'
    opener on the correction. Documented as lower precision, higher recall.
    """
    text = text or ''
    out = []
    for s, e, kind in segments(text):
        if kind != 'narr':
            continue
        frag = text[s:e]
        for m in _DENIAL_CORRECTION.finditer(frag):
            span = frag[m.start():m.end()].strip()
            if _SCALAR_RE.search(span) or _META_RE.search(span):
                continue
            # the correction half must be affirmative; otherwise this is a negation run
            head = re.split(r"[;.!?\u2014\u2013]", span, maxsplit=1)
            tail = span[len(head[0]):] if len(head) > 1 else ''
            if not tail.strip() or _NEG_IN_CLAUSE.search(tail):
                continue
            out.append({'start': s + m.start(), 'end': s + m.end(), 'span': span})
    return out


# --- comma-appended absolute trailers (trailer_rate / two_tics) ---------------
# A trailer opens with a possessive/determiner+noun, a bare body noun, an -ing participle or
# an -ly adverb, and carries no finite verb (the absolute/participial appositive).
_BODY_NOUN = (r"(?:head|eyes|gaze|hands?|fingers?|lips?|mouth|jaw|breath|shoulders?|chest|back|"
              r"voice|body|frame|face|brow|hair|legs?|arms?|knees?|pulse|heart|throat)")
_TRAILER = re.compile(
    r",\s*(?:" + _BODY_NOUN + r"\b|[a-z]+ing\b|[a-z]+ly\b|"
    r"(?:her|his|its|their|the|a|an)\s+\w+)", re.I)
_STACK = re.compile(
    r",\s*(?:" + _BODY_NOUN + r"\b|\w+ing\b|\w+ly\b|(?:her|his|its|their)\s+\w+)", re.I)


def trailer_hits(text):
    """Comma-appended absolute/participial trailers, per sentence."""
    out = []
    for sent in split_sentences(text, min_words=5):
        hits = _TRAILER.findall(sent)
        if not hits:
            continue
        out.append({
            'span': sent,
            'n_trailers': len(hits),
            'stacked': int(len(_STACK.findall(sent)) >= 2 and sent.count(',') >= 3),
        })
    return out


def question_handback(text):
    """True when the turn's FINAL sentence ends on a question mark.

    Uses every sentence (``min_words=1``) because a handback can be short --
    "What do you think?" is four words, and a 5-word floor would silently drop the
    very case the metric exists to catch. Trailing quote/asterisk/bracket glyphs are
    stripped first so '"...anything else?"' still counts.
    """
    sents = split_sentences(text)
    if not sents:
        return False
    return sents[-1].rstrip('"*\u201d)\u00bb ').endswith('?')


# --- per-response bundle ----------------------------------------------------
def response_metrics(text, context_text='', last_user_text=''):
    """Every metric for one assistant response, as a flat dict of numbers."""
    m = {}
    c = comma_stats([text])
    m['commas_per_sent'] = round(c['commas_per_sent'], 3)
    m['commas_per_100w'] = round(c['commas_per_100w'], 3)
    m['sents_3plus_commas_pct'] = round(c['sents_3plus_commas_pct'], 2)
    t = tail_positivity(text)
    m['tail_affect_lift'] = round(t['affect_lift'], 3)
    m['tail_sig_lift'] = round(t['sig_lift'], 3)
    m['tail_but_pivot_final'] = round(t['but_pivot_final'], 2)
    d = depth_proxies(text, context_text, last_user_text)
    m['grounding_per_100w'] = round(d['grounding_per_100w'], 2)
    m['narration_pct'] = round(d['narration_pct'], 2)
    m['interior_per_100w'] = round(d['interior_per_100w'], 2)
    m['words'] = d['words']
    cd = coherence_defects(text)
    m['defect_script_mix'] = cd['script_mix']
    m['defect_foreign_article'] = cd['foreign_article']
    m['defect_det_stutter'] = cd['det_stutter']
    m['defect_dup_sentence'] = cd['dup_sentence']
    m['defect_any'] = cd['any']
    n = negation_profile(text)
    m['narr_neg_per_100_sent'] = round(n['neg_per_100_sent'], 2)
    m['narr_neg_run_ge2'] = n['run_ge2']
    m['narr_neg_max_run'] = n['max_run']
    m['narr_neg_bare_pct'] = round(n['bare_pct'], 2)
    m['narr_neg_ellipsis_pct'] = round(n['ellipsis_pct'], 2)
    m['denial_correction'] = len(denial_correction_hits(text))
    m['absolute_trailer'] = sum(1 for h in trailer_hits(text) if not h['stacked'])
    m['qualification_stack'] = sum(1 for h in trailer_hits(text) if h['stacked'])
    m['question_handback'] = int(question_handback(text))
    return m


UNMEASURED = -1.0


def aggregate(metric_rows):
    """Mean of every numeric key across rows (the arm-table analogue).

    Values equal to ``UNMEASURED`` (-1.0, used by grounding when no context was
    supplied) are excluded rather than averaged in, so an arm with missing context
    does not read as an arm with zero grounding.
    """
    keys = sorted({k for r in metric_rows for k in r})
    out = {}
    for k in keys:
        vals = [r[k] for r in metric_rows
                if isinstance(r.get(k), (int, float)) and r[k] != UNMEASURED]
        out[k] = round(sum(vals) / len(vals), 3) if vals else UNMEASURED
    out['n'] = len(metric_rows)
    return out
