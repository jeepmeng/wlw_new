import ahocorasick
from typing import List, Tuple, Dict, Optional
from elasticsearch import AsyncElasticsearch

PLACEHOLDER = "[已屏蔽]"

class SensitiveFilterAC:
    def __init__(self, es: AsyncElasticsearch, index: str, ignore_case: bool,
                 page_size: int = 2000, max_single_fetch: int = 10_000):
        self.es = es
        self.index = index
        self.ignore_case = ignore_case
        self.page_size = page_size
        self.max_single_fetch = max_single_fetch
        self.A: Optional[ahocorasick.Automaton] = None
        self.version_tag: str = ""
        self.max_pat_len: int = 1

    async def _count_active(self) -> int:
        body = {"size": 0, "track_total_hits": True,
                "query": {"bool": {"filter": [{"term": {"is_active": True}}]}}}
        resp = await self.es.search(index=self.index, body=body)
        return int(resp["hits"]["total"]["value"])

    async def _fetch_full(self) -> List[Dict]:
        body = {"size": self.max_single_fetch,
                "query": {"bool": {"filter": [{"term": {"is_active": True}}]}},
                "_source": ["term", "norm", "updated_at"]}
        resp = await self.es.search(index=self.index, body=body, request_timeout=30)
        return resp["hits"]["hits"]

    async def _fetch_paged(self) -> List[Dict]:
        search_after = None
        out: List[Dict] = []
        while True:
            body = {"size": self.page_size,
                    "sort": [{"updated_at": "asc"}, {"_id": "asc"}],
                    "query": {"bool": {"filter": [{"term": {"is_active": True}}]}},
                    "_source": ["term", "norm", "updated_at"]}
            if search_after:
                body["search_after"] = search_after
            resp = await self.es.search(index=self.index, body=body, request_timeout=30)
            hits = resp["hits"]["hits"]
            if not hits: break
            out.extend(hits)
            search_after = hits[-1]["sort"]
        return out

    async def refresh(self) -> int:
        total = await self._count_active()
        if total == 0:
            self.A = None
            self.version_tag = "0:"
            self.max_pat_len = 1
            return 0

        hits = await (self._fetch_full() if total <= self.max_single_fetch else self._fetch_paged())
        # terms: List[str], latest = [], ""
        terms: List[str] = []
        latest: str = ""
        for h in hits:
            s = h["_source"]
            t = (s.get("norm") or s.get("term") or "").strip()
            if not t: continue
            t = t.lower() if self.ignore_case else t
            terms.append(t)
            if s.get("updated_at", "") > latest: latest = s["updated_at"]

        uniq = sorted(set(terms), key=len, reverse=True)
        A = ahocorasick.Automaton()
        for t in uniq:
            A.add_word(t, t)
        A.make_automaton()
        self.A = A
        self.max_pat_len = max((len(t) for t in uniq), default=1)
        self.version_tag = f"{len(uniq)}:{latest}"
        return len(uniq)

    def detect(self, text: str) -> List[Dict]:
        if not text or self.A is None: return []
        view = text.lower() if self.ignore_case else text
        spans: List[Dict] = []
        for end, word in self.A.iter(view):
            start = end - len(word) + 1
            spans.append({"start": start, "end": end + 1, "word": text[start:end+1]})
        return _merge_spans(spans)

    def mask(self, text: str) -> Tuple[str, List[Dict]]:
        spans = self.detect(text)
        if not spans: return text, []
        return _mask_text(text, spans), spans


def _merge_spans(spans: List[Dict]) -> List[Dict]:
    if not spans: return spans
    spans = sorted(spans, key=lambda s: (s["start"], s["end"]))
    out = [spans[0]]
    for s in spans[1:]:
        last = out[-1]
        if s["start"] <= last["end"]:
            last["end"] = max(last["end"], s["end"])
            last["word"] = ""
        else:
            out.append(s)
    return out


def _mask_text(text: str, spans: List[Dict], placeholder: str = PLACEHOLDER) -> str:
    res, last = [], 0
    for sp in spans:
        res.append(text[last:sp["start"]])
        res.append(placeholder)
        last = sp["end"]
    res.append(text[last:])
    return "".join(res)