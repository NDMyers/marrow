#!/usr/bin/env python3
"""cc_audit.py - Claude Code token & cost auditor.

Reads a Claude Code session transcript (JSONL) and prints a Copilot-style
"agent debug log": per-request token breakdown (fresh input / cached-read input /
cache-write input / output), per-model cost using *live-fetched* current API
pricing, and the tool-call chain, plus session totals.

Stdlib only (Python 3.8+). No pip install required.

Examples:
    python tools/cc_audit.py --list
    python tools/cc_audit.py --latest
    python tools/cc_audit.py --session 70fdb069
    python tools/cc_audit.py --session f51198f7 --markdown report.md --verbose
    python tools/cc_audit.py --refresh --latest

See tools/README.md for details.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

LITELLM_URL = (
    "https://raw.githubusercontent.com/BerriAI/litellm/main/"
    "model_prices_and_context_window.json"
)
HERE = Path(__file__).resolve().parent
CACHE_DIR = HERE / ".cache"
CACHE_FILE = CACHE_DIR / "litellm_prices.json"
OVERRIDE_FILE = HERE / "pricing_overrides.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # reuse cached pricing for 24h

# Default Claude Code projects root. The project slug is derived from the
# current working directory the same way Claude Code does (path separators
# and colons folded to dashes), so the tool works from any repo checkout.
DEFAULT_PROJECTS_ROOT = Path.home() / ".claude" / "projects"


def derive_project_slug(path: Path | None = None) -> str:
    raw = str((path or Path.cwd()).resolve())
    return re.sub(r"[:\\/.]", "-", raw)


DEFAULT_PROJECT_SLUG = derive_project_slug()

# The five rate fields we care about (per token, USD).
RATE_KEYS = (
    "input_cost_per_token",
    "output_cost_per_token",
    "cache_read_input_token_cost",
    "cache_creation_input_token_cost",
    "cache_creation_input_token_cost_above_1hr",
)


def _mk_rates(inp, out, read, write5m, write1h):
    return {
        "input_cost_per_token": inp,
        "output_cost_per_token": out,
        "cache_read_input_token_cost": read,
        "cache_creation_input_token_cost": write5m,
        "cache_creation_input_token_cost_above_1hr": write1h,
    }


# Bundled last-resort defaults (per-token USD), used only if both the live fetch
# and the cache are unavailable AND no override covers the model. Sourced from the
# official pricing page (June 2026): per-million / 1e6.
BUNDLED_DEFAULTS = {
    # family-version : rates
    "claude-opus-4-8": _mk_rates(5e-6, 25e-6, 0.5e-6, 6.25e-6, 10e-6),
    "claude-opus-4-7": _mk_rates(5e-6, 25e-6, 0.5e-6, 6.25e-6, 10e-6),
    "claude-sonnet-4-6": _mk_rates(3e-6, 15e-6, 0.3e-6, 3.75e-6, 6e-6),
    "claude-sonnet-4-5": _mk_rates(3e-6, 15e-6, 0.3e-6, 3.75e-6, 6e-6),
    "claude-haiku-4-5": _mk_rates(1e-6, 5e-6, 0.1e-6, 1.25e-6, 2e-6),
}

# ANSI colors (disabled when not a tty or NO_COLOR set).
_USE_COLOR = sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def c(text, code):
    if not _USE_COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"


def bold(t):
    return c(t, "1")


def dim(t):
    return c(t, "2")


def cyan(t):
    return c(t, "36")


def green(t):
    return c(t, "32")


def yellow(t):
    return c(t, "33")


def red(t):
    return c(t, "31")


# --------------------------------------------------------------------------- #
# Pricing
# --------------------------------------------------------------------------- #


def fetch_pricing(refresh=False, quiet=False):
    """Return the raw LiteLLM pricing dict, fetching/caching as needed.

    Order: fresh cache (<24h) -> live fetch -> stale cache -> {}.
    """
    now = time.time()
    if not refresh and CACHE_FILE.exists():
        age = now - CACHE_FILE.stat().st_mtime
        if age < CACHE_TTL_SECONDS:
            try:
                return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                pass  # fall through to refetch

    # Try live fetch.
    try:
        req = urllib.request.Request(
            LITELLM_URL, headers={"User-Agent": "cc_audit/1.0"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(json.dumps(data), encoding="utf-8")
        if not quiet:
            print(dim(f"[pricing] fetched {len(data)} entries from LiteLLM"),
                  file=sys.stderr)
        return data
    except Exception as exc:  # network error, bad JSON, etc.
        if CACHE_FILE.exists():
            if not quiet:
                print(yellow(f"[pricing] live fetch failed ({exc}); "
                             f"using stale cache"), file=sys.stderr)
            try:
                return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                pass
        if not quiet:
            print(yellow(f"[pricing] live fetch failed ({exc}); "
                         f"no cache -> bundled defaults only"), file=sys.stderr)
        return {}


def load_overrides():
    """Return the user-maintained override dict, or {} if absent/invalid."""
    if not OVERRIDE_FILE.exists():
        return {}
    try:
        raw = json.loads(OVERRIDE_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(yellow(f"[pricing] ignoring bad {OVERRIDE_FILE.name}: {exc}"),
              file=sys.stderr)
        return {}
    # Drop comment-ish keys (anything starting with "_").
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def _normalize_candidates(model_id):
    """Yield candidate lookup keys for a bare transcript model id."""
    mid = model_id.strip()
    yield mid
    yield f"anthropic/{mid}"
    yield f"anthropic.{mid}"
    # strip a date suffix like -20251001
    parts = mid.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 8:
        yield parts[0]
        yield f"anthropic.{parts[0]}"


def _family_version(model_id):
    """Reduce e.g. 'claude-haiku-4-5-20251001' -> 'claude-haiku-4-5'."""
    toks = model_id.split("-")
    out = []
    for t in toks:
        if t.isdigit() and len(t) == 8:  # date stamp
            break
        out.append(t)
    return "-".join(out)


class PriceBook:
    """Resolves a transcript model id to its five per-token rates."""

    def __init__(self, litellm, overrides):
        self.litellm = litellm or {}
        self.overrides = overrides or {}
        self._cache = {}
        self.unmatched = set()

    def _extract(self, entry):
        if not isinstance(entry, dict):
            return None
        if "input_cost_per_token" not in entry:
            return None
        rates = {}
        for k in RATE_KEYS:
            rates[k] = entry.get(k)
        # Fallbacks for missing cache fields.
        if rates["cache_read_input_token_cost"] is None:
            rates["cache_read_input_token_cost"] = (
                rates["input_cost_per_token"] * 0.1
                if rates["input_cost_per_token"] is not None else None
            )
        if rates["cache_creation_input_token_cost"] is None:
            rates["cache_creation_input_token_cost"] = (
                rates["input_cost_per_token"] * 1.25
                if rates["input_cost_per_token"] is not None else None
            )
        if rates["cache_creation_input_token_cost_above_1hr"] is None:
            rates["cache_creation_input_token_cost_above_1hr"] = (
                rates["input_cost_per_token"] * 2.0
                if rates["input_cost_per_token"] is not None else None
            )
        return rates

    def resolve(self, model_id):
        if model_id in self._cache:
            return self._cache[model_id]
        rates, source = self._resolve_inner(model_id)
        if rates is None:
            self.unmatched.add(model_id)
        self._cache[model_id] = (rates, source)
        return rates, source

    def _resolve_inner(self, model_id):
        if not model_id:
            return None, None
        # 1. exact override
        if model_id in self.overrides:
            r = self._extract(self.overrides[model_id])
            if r:
                return r, "override"
        fam = _family_version(model_id)
        if fam in self.overrides:
            r = self._extract(self.overrides[fam])
            if r:
                return r, "override"
        # 2. litellm exact / prefixed candidates
        for cand in _normalize_candidates(model_id):
            if cand in self.litellm:
                r = self._extract(self.litellm[cand])
                if r:
                    return r, "litellm"
        # 3. litellm substring match on family-version
        if fam:
            for key, entry in self.litellm.items():
                if fam in key:
                    r = self._extract(entry)
                    if r:
                        return r, f"litellm~{key}"
        # 4. bundled defaults by family-version
        if fam in BUNDLED_DEFAULTS:
            return BUNDLED_DEFAULTS[fam], "bundled"
        return None, None


# --------------------------------------------------------------------------- #
# Transcript parsing
# --------------------------------------------------------------------------- #


class Request:
    """One assistant message that carries a usage object."""

    __slots__ = (
        "index", "timestamp", "model", "stop_reason",
        "input_tokens", "output_tokens", "cache_read", "cache_write_5m",
        "cache_write_1h", "web_search", "web_fetch",
        "thinking_chars", "text_chars", "tool_calls", "raw_usage",
        "is_subagent",
    )

    def __init__(self):
        self.index = 0
        self.timestamp = None
        self.model = None
        self.stop_reason = None
        self.is_subagent = False
        self.input_tokens = 0
        self.output_tokens = 0
        self.cache_read = 0
        self.cache_write_5m = 0
        self.cache_write_1h = 0
        self.web_search = 0
        self.web_fetch = 0
        self.thinking_chars = 0
        self.text_chars = 0
        self.tool_calls = []  # list of dicts: {name, input, id, result_chars, is_error}
        self.raw_usage = {}


def _content_to_text(content):
    """Flatten a tool_result content field (str or list of blocks) to text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for b in content:
            if isinstance(b, dict):
                if b.get("type") == "text":
                    parts.append(b.get("text", ""))
                else:
                    parts.append(json.dumps(b))
            else:
                parts.append(str(b))
        return "".join(parts)
    if content is None:
        return ""
    return str(content)


def subagent_files(path):
    """Sub-agent transcripts for a session live in <stem>/subagents/*.jsonl."""
    subdir = Path(path).parent / Path(path).stem / "subagents"
    if subdir.is_dir():
        return sorted(subdir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
    return []


def _parse_file(path, state, is_subagent):
    """Parse one JSONL transcript into the shared `state` dict.

    Claude Code splits a single assistant message (one `message.id`) across
    multiple JSONL lines -- one content block per line -- and repeats the full
    `usage` on every line. We group by message id: usage is taken once per id,
    while content blocks (thinking/text/tool_use) are merged across all its
    lines. Counting per-line would multiply tokens/cost 2-4x.
    """
    requests = state["requests"]
    tool_by_id = state["tool_by_id"]
    req_by_id = state["req_by_id"]

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            ttype = obj.get("type")

            if ttype == "assistant":
                msg = obj.get("message") or {}
                usage = msg.get("usage")
                if not isinstance(usage, dict):
                    continue
                mid = msg.get("id")
                if not mid:
                    state["anon"] += 1
                    mid = f"__anon_{state['anon']}"
                # Sub-agent files are isolated namespaces; never merge a
                # sub-agent message into a parent one with a colliding id.
                key = (path, mid) if is_subagent else mid

                r = req_by_id.get(key)
                if r is None:
                    # First line for this message id: record usage exactly once.
                    r = Request()
                    r.index = len(requests) + 1
                    r.is_subagent = is_subagent
                    r.timestamp = obj.get("timestamp")
                    r.model = msg.get("model")
                    r.stop_reason = msg.get("stop_reason")
                    r.raw_usage = usage
                    r.input_tokens = usage.get("input_tokens", 0) or 0
                    r.output_tokens = usage.get("output_tokens", 0) or 0
                    r.cache_read = usage.get("cache_read_input_tokens", 0) or 0
                    cc = usage.get("cache_creation") or {}
                    if cc:
                        r.cache_write_5m = cc.get("ephemeral_5m_input_tokens", 0) or 0
                        r.cache_write_1h = cc.get("ephemeral_1h_input_tokens", 0) or 0
                    else:
                        # older format: lump everything as 5m write
                        r.cache_write_5m = usage.get(
                            "cache_creation_input_tokens", 0) or 0
                    stu = usage.get("server_tool_use") or {}
                    r.web_search = stu.get("web_search_requests", 0) or 0
                    r.web_fetch = stu.get("web_fetch_requests", 0) or 0
                    req_by_id[key] = r
                    requests.append(r)
                else:
                    # Later line of the same message: keep a later stop_reason.
                    if msg.get("stop_reason"):
                        r.stop_reason = msg.get("stop_reason")

                # Content blocks differ per line -> always merge them in.
                for block in msg.get("content", []) or []:
                    if not isinstance(block, dict):
                        continue
                    bt = block.get("type")
                    if bt == "thinking":
                        r.thinking_chars += len(block.get("thinking", "") or "")
                    elif bt == "text":
                        r.text_chars += len(block.get("text", "") or "")
                    elif bt == "tool_use":
                        tc = {
                            "name": block.get("name"),
                            "input": block.get("input") or {},
                            "id": block.get("id"),
                            "result_chars": None,
                            "is_error": False,
                        }
                        r.tool_calls.append(tc)
                        if tc["id"]:
                            tool_by_id[tc["id"]] = tc

            elif ttype == "user":
                msg = obj.get("message") or {}
                content = msg.get("content")
                if isinstance(content, list):
                    for block in content:
                        if (isinstance(block, dict)
                                and block.get("type") == "tool_result"):
                            tid = block.get("tool_use_id")
                            tc = tool_by_id.get(tid)
                            if tc is not None:
                                txt = _content_to_text(block.get("content"))
                                tc["result_chars"] = len(txt)
                                tc["is_error"] = bool(block.get("is_error"))


def load_session(path, include_subagents=True):
    """Parse a transcript (and, by default, its sub-agent transcripts) into an
    ordered list of Request objects.

    Sub-agent runs (Task/Agent tool, e.g. Explore) are billed to the session but
    stored separately under <session>/subagents/*.jsonl. They must be included or
    a session that delegates work will be massively under-counted.
    """
    state = {"requests": [], "tool_by_id": {}, "req_by_id": {}, "anon": 0}
    _parse_file(path, state, is_subagent=False)
    if include_subagents:
        for f in subagent_files(path):
            _parse_file(f, state, is_subagent=True)
    return state["requests"]


# --------------------------------------------------------------------------- #
# Costing
# --------------------------------------------------------------------------- #


def cost_for_request(r, pricebook):
    """Return (cost_float_or_None, source_str) for one request."""
    rates, source = pricebook.resolve(r.model)
    if rates is None:
        return None, None
    cost = 0.0
    cost += r.input_tokens * (rates["input_cost_per_token"] or 0)
    cost += r.output_tokens * (rates["output_cost_per_token"] or 0)
    cost += r.cache_read * (rates["cache_read_input_token_cost"] or 0)
    # Both cache-write buckets are priced at the standard cache-creation rate.
    # Claude Code uses a 1h cache TTL but Anthropic's reported cost
    # (CLI total_cost_usd) bills those writes at 1.25x, not the 2x "above_1hr"
    # tier -- verified by reconciling against the CLI to <1%. The 1h token
    # COUNT is still tracked/shown; only its price uses the standard rate.
    cwrate = rates["cache_creation_input_token_cost"] or 0
    cost += (r.cache_write_5m + r.cache_write_1h) * cwrate
    return cost, source


# --------------------------------------------------------------------------- #
# Session discovery
# --------------------------------------------------------------------------- #


def project_dir(slug, projects_root):
    return Path(projects_root) / slug


def list_sessions(slug, projects_root):
    d = project_dir(slug, projects_root)
    if not d.is_dir():
        return []
    files = sorted(d.glob("*.jsonl"), key=lambda p: p.stat().st_mtime,
                   reverse=True)
    return files


def resolve_ident(ident, project, projects_root):
    """Resolve one session identifier (file path or uuid prefix) to a Path."""
    p = Path(ident)
    if p.is_file():
        return p
    sessions = list_sessions(project, projects_root)
    matches = [s for s in sessions if s.stem.startswith(ident)]
    if not matches:
        sys.exit(red(f"no session in {project} matches '{ident}'"))
    if len(matches) > 1:
        names = ", ".join(s.stem for s in matches)
        sys.exit(red(f"ambiguous session prefix '{ident}': {names}"))
    return matches[0]


def resolve_session_file(args):
    """Determine which transcript file to analyze from CLI args."""
    if args.file:
        p = Path(args.file)
        if not p.is_file():
            sys.exit(red(f"file not found: {p}"))
        return p
    sessions = list_sessions(args.project, args.projects_root)
    if args.session:
        # match by uuid prefix
        matches = [p for p in sessions if p.stem.startswith(args.session)]
        if not matches:
            sys.exit(red(f"no session in {args.project} matches "
                         f"'{args.session}'"))
        if len(matches) > 1:
            names = ", ".join(p.stem for p in matches)
            sys.exit(red(f"ambiguous session prefix '{args.session}': {names}"))
        return matches[0]
    # default: latest
    if not sessions:
        sys.exit(red(f"no sessions found in project '{args.project}' "
                     f"under {args.projects_root}"))
    return sessions[0]


# --------------------------------------------------------------------------- #
# Formatting helpers
# --------------------------------------------------------------------------- #


def fmt_int(n):
    return f"{n:,}"


def fmt_usd(x):
    if x is None:
        return "?"
    if x >= 1:
        return f"${x:,.2f}"
    if x >= 0.01:
        return f"${x:.4f}"
    return f"${x:.6f}"


def fmt_ts(ts):
    if not ts:
        return "-"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone().strftime("%H:%M:%S")
    except (ValueError, AttributeError):
        return str(ts)


def parse_ts(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def short_args(inp, limit=80):
    if not inp:
        return ""
    try:
        s = json.dumps(inp, separators=(",", ":"))
    except (TypeError, ValueError):
        s = str(inp)
    if len(s) > limit:
        s = s[: limit - 1] + "…"
    return s


def human_chars(n):
    if n is None:
        return "-"
    if n < 1000:
        return f"{n}c"
    if n < 1_000_000:
        return f"{n/1000:.1f}Kc"
    return f"{n/1_000_000:.1f}Mc"


# --------------------------------------------------------------------------- #
# Report rendering
# --------------------------------------------------------------------------- #


class Out:
    """Tees output to stdout (with color) and optionally a markdown file (plain)."""

    def __init__(self, md_path=None):
        self.lines = []
        self.md_path = md_path

    def p(self, s=""):
        print(s)
        if self.md_path is not None:
            self.lines.append(_strip_ansi(s))

    def flush(self):
        if self.md_path is not None:
            body = "\n".join(self.lines)
            # Wrap in a fence so the monospace report renders intact in viewers.
            Path(self.md_path).write_text(
                "```\n" + body + "\n```\n", encoding="utf-8")


def _strip_ansi(s):
    out = []
    i = 0
    while i < len(s):
        if s[i] == "\033":
            j = s.find("m", i)
            if j != -1:
                i = j + 1
                continue
        out.append(s[i])
        i += 1
    return "".join(out)


def render_report(out, path, requests, pricebook, verbose=False):
    models = sorted({r.model for r in requests if r.model})
    times = [parse_ts(r.timestamp) for r in requests]
    times = [t for t in times if t]
    start = min(times) if times else None
    end = max(times) if times else None
    n_tools = sum(len(r.tool_calls) for r in requests)

    # totals
    tot = dict(inp=0, out=0, read=0, w5m=0, w1h=0, cost=0.0, web_search=0,
               web_fetch=0)
    per_model = {}  # model -> {req, cost, inp, out, read, write}
    tool_freq = {}
    cost_unknown = False

    out.p()
    out.p(bold("=" * 78))
    out.p(bold(" Claude Code Session Audit"))
    out.p(bold("=" * 78))
    out.p(f"  file       : {path}")
    out.p(f"  session    : {Path(path).stem}")
    out.p(f"  models     : {', '.join(models) if models else '(none)'}")
    if start and end:
        dur = end - start
        out.p(f"  span       : {start.astimezone():%Y-%m-%d %H:%M:%S} "
              f"-> {end.astimezone():%H:%M:%S}  ({dur})")
    out.p(f"  requests   : {len(requests)}   tool calls: {n_tools}")
    out.p("")

    # Per-request detail
    out.p(bold("-" * 78))
    out.p(bold(" Per-request breakdown"))
    out.p(dim("   in=fresh input  rd=cache-read  wr=cache-write(5m+1h)  out=output"))
    out.p(bold("-" * 78))

    for r in requests:
        cost, source = cost_for_request(r, pricebook)
        if cost is None:
            cost_unknown = True
        # accumulate
        tot["inp"] += r.input_tokens
        tot["out"] += r.output_tokens
        tot["read"] += r.cache_read
        tot["w5m"] += r.cache_write_5m
        tot["w1h"] += r.cache_write_1h
        tot["web_search"] += r.web_search
        tot["web_fetch"] += r.web_fetch
        if cost is not None:
            tot["cost"] += cost
        pm = per_model.setdefault(
            r.model or "(unknown)",
            dict(req=0, cost=0.0, inp=0, out=0, read=0, write=0))
        pm["req"] += 1
        pm["inp"] += r.input_tokens
        pm["out"] += r.output_tokens
        pm["read"] += r.cache_read
        pm["write"] += r.cache_write_5m + r.cache_write_1h
        if cost is not None:
            pm["cost"] += cost

        wr = r.cache_write_5m + r.cache_write_1h
        model_short = (r.model or "?").replace("claude-", "")
        head = (f"  {cyan('#'+str(r.index)):<6} {fmt_ts(r.timestamp)} "
                f"{model_short:<22} "
                f"in={fmt_int(r.input_tokens):>7} "
                f"rd={fmt_int(r.cache_read):>9} "
                f"wr={fmt_int(wr):>8} "
                f"out={fmt_int(r.output_tokens):>6}  "
                f"{green(fmt_usd(cost)):>10}")
        out.p(head)
        if r.cache_write_5m or r.cache_write_1h:
            out.p(dim(f"          cache-write split: 5m={fmt_int(r.cache_write_5m)} "
                      f"1h={fmt_int(r.cache_write_1h)}"))
        if r.web_search or r.web_fetch:
            out.p(dim(f"          server tools: web_search={r.web_search} "
                      f"web_fetch={r.web_fetch}"))
        # tool chain
        for tc in r.tool_calls:
            tool_freq[tc["name"]] = tool_freq.get(tc["name"], 0) + 1
            flag = red(" [ERROR]") if tc["is_error"] else ""
            res = (f" -> {human_chars(tc['result_chars'])}"
                   if tc["result_chars"] is not None else "")
            arglim = 200 if verbose else 80
            out.p(f"          {dim('|')} {yellow(tc['name'] or '?')}"
                  f"({dim(short_args(tc['input'], arglim))}){res}{flag}")

    # Totals
    out.p("")
    out.p(bold("-" * 78))
    out.p(bold(" Token totals"))
    out.p(bold("-" * 78))
    out.p(f"  fresh input        : {fmt_int(tot['inp']):>14}")
    out.p(f"  cached-read input  : {fmt_int(tot['read']):>14}")
    out.p(f"  cache-write (5m)   : {fmt_int(tot['w5m']):>14}")
    out.p(f"  cache-write (1h)   : {fmt_int(tot['w1h']):>14}")
    out.p(f"  output             : {fmt_int(tot['out']):>14}")
    total_in_all = tot["inp"] + tot["read"] + tot["w5m"] + tot["w1h"]
    out.p(dim(f"  (all input combined: {fmt_int(total_in_all)})"))
    if tot["web_search"] or tot["web_fetch"]:
        out.p(f"  server web_search  : {fmt_int(tot['web_search']):>14}")
        out.p(f"  server web_fetch   : {fmt_int(tot['web_fetch']):>14}")

    # Cost split
    out.p("")
    out.p(bold("-" * 78))
    out.p(bold(" Cost"))
    out.p(bold("-" * 78))
    cat = _cost_by_category(requests, pricebook)
    out.p(f"  fresh input        : {fmt_usd(cat['inp']):>12}")
    out.p(f"  cached-read input  : {fmt_usd(cat['read']):>12}")
    out.p(f"  cache-write        : {fmt_usd(cat['write']):>12}")
    out.p(f"  output             : {fmt_usd(cat['out']):>12}")
    out.p(bold(f"  TOTAL              : {green(fmt_usd(tot['cost'])):>12}"))
    if cost_unknown:
        out.p(yellow("  (some requests had no matching price -> excluded from "
                     "TOTAL; see warnings)"))

    # Cost by model
    out.p("")
    out.p(bold(" Cost by model"))
    for model in sorted(per_model, key=lambda m: -per_model[m]["cost"]):
        pm = per_model[model]
        out.p(f"  {model:<32} {pm['req']:>4} req  "
              f"{green(fmt_usd(pm['cost'])):>12}")

    # Tool frequency
    if tool_freq:
        out.p("")
        out.p(bold(" Tool-call frequency"))
        for name in sorted(tool_freq, key=lambda n: -tool_freq[n]):
            out.p(f"  {name:<40} {tool_freq[name]:>4}")

    if pricebook.unmatched:
        out.p("")
        out.p(yellow("  Unpriced models (no rate found): "
                     + ", ".join(sorted(pricebook.unmatched))))
    out.p("")


def _cost_by_category(requests, pricebook):
    cat = dict(inp=0.0, out=0.0, read=0.0, write=0.0)
    for r in requests:
        rates, _ = pricebook.resolve(r.model)
        if rates is None:
            continue
        cat["inp"] += r.input_tokens * (rates["input_cost_per_token"] or 0)
        cat["out"] += r.output_tokens * (rates["output_cost_per_token"] or 0)
        cat["read"] += r.cache_read * (rates["cache_read_input_token_cost"] or 0)
        cat["write"] += (
            (r.cache_write_5m + r.cache_write_1h)
            * (rates["cache_creation_input_token_cost"] or 0))
    return cat


def render_list(slug, projects_root, pricebook):
    sessions = list_sessions(slug, projects_root)
    if not sessions:
        print(red(f"no sessions found in project '{slug}' under {projects_root}"))
        return
    print(bold(f"\nSessions in {slug}  ({len(sessions)})\n"))
    print(f"  {'session':<14} {'modified':<17} {'req':>4} {'cost':>10}  models")
    print("  " + "-" * 72)
    for p in sessions:
        try:
            requests = load_session(p)
        except OSError:
            continue
        total = 0.0
        for r in requests:
            cost, _ = cost_for_request(r, pricebook)
            if cost:
                total += cost
        models = sorted({(r.model or "?").replace("claude-", "")
                         for r in requests if r.model})
        mtime = datetime.fromtimestamp(p.stat().st_mtime)
        print(f"  {p.stem[:13]:<14} {mtime:%Y-%m-%d %H:%M} "
              f"{len(requests):>4} {green(fmt_usd(total)):>10}  "
              f"{', '.join(models)}")
    print()


# --------------------------------------------------------------------------- #
# Comparison (A/B)
# --------------------------------------------------------------------------- #


def summarize(requests, pricebook):
    """Aggregate a session into token/cost/tool totals for comparison."""
    s = dict(n_req=len(requests), n_tools=0, inp=0, out=0, read=0, w5m=0,
             w1h=0, web_search=0, web_fetch=0, tool_freq={}, models=set(),
             unpriced=False)
    for r in requests:
        s["inp"] += r.input_tokens
        s["out"] += r.output_tokens
        s["read"] += r.cache_read
        s["w5m"] += r.cache_write_5m
        s["w1h"] += r.cache_write_1h
        s["web_search"] += r.web_search
        s["web_fetch"] += r.web_fetch
        s["n_tools"] += len(r.tool_calls)
        if r.model:
            s["models"].add(r.model)
        for tc in r.tool_calls:
            s["tool_freq"][tc["name"]] = s["tool_freq"].get(tc["name"], 0) + 1
        cost, _ = cost_for_request(r, pricebook)
        if cost is None:
            s["unpriced"] = True
    cat = _cost_by_category(requests, pricebook)
    s["cost_inp"] = cat["inp"]
    s["cost_out"] = cat["out"]
    s["cost_read"] = cat["read"]
    s["cost_write"] = cat["write"]
    s["cost_total"] = cat["inp"] + cat["out"] + cat["read"] + cat["write"]
    s["write"] = s["w5m"] + s["w1h"]
    s["total_input"] = s["inp"] + s["read"] + s["w5m"] + s["w1h"]
    return s


def _delta_pct(a, b):
    if a == 0:
        return "—" if b == 0 else "  new"
    return f"{(b - a) / a * 100:+.1f}%"


def _color_dir(line, d):
    """Green when B<A (reduction = good for a benchmark), yellow when up."""
    if d < 0:
        return green(line)
    if d > 0:
        return yellow(line)
    return dim(line)


def render_compare(out, path_a, sa, path_b, sb):
    LW, VW, DW, PW = 22, 16, 16, 10

    def trow(label, a, b):
        d = b - a
        dtxt = f"{d:+,}"
        line = (f"  {label:<{LW}}{fmt_int(a):>{VW}}{fmt_int(b):>{VW}}"
                f"{dtxt:>{DW}}{_delta_pct(a, b):>{PW}}")
        out.p(_color_dir(line, d))

    def crow(label, a, b, boldit=False):
        d = b - a
        dtxt = ("+" if d >= 0 else "-") + fmt_usd(abs(d))
        line = (f"  {label:<{LW}}{fmt_usd(a):>{VW}}{fmt_usd(b):>{VW}}"
                f"{dtxt:>{DW}}{_delta_pct(a, b):>{PW}}")
        line = bold(line) if boldit else _color_dir(line, d)
        out.p(line)

    out.p()
    out.p(bold("=" * 80))
    out.p(bold(" Claude Code Session Comparison  (A = baseline, B = candidate)"))
    out.p(bold("=" * 80))
    out.p(f"  A = {Path(path_a).stem}")
    out.p(dim(f"      {', '.join(sorted(sa['models'])) or '(none)'}"
              f"   {sa['n_req']} requests"))
    out.p(f"  B = {Path(path_b).stem}")
    out.p(dim(f"      {', '.join(sorted(sb['models'])) or '(none)'}"
              f"   {sb['n_req']} requests"))
    out.p("")
    out.p(bold(f"  {'metric':<{LW}}{'A':>{VW}}{'B':>{VW}}"
               f"{'Δ (B-A)':>{DW}}{'Δ%':>{PW}}"))
    out.p("  " + "-" * 78)

    out.p(bold(" tokens"))
    trow("fresh input", sa["inp"], sb["inp"])
    trow("cached-read input", sa["read"], sb["read"])
    trow("cache-write", sa["write"], sb["write"])
    trow("output", sa["out"], sb["out"])
    trow("total input (all)", sa["total_input"], sb["total_input"])

    out.p(bold(" cost"))
    crow("fresh input", sa["cost_inp"], sb["cost_inp"])
    crow("cached-read input", sa["cost_read"], sb["cost_read"])
    crow("cache-write", sa["cost_write"], sb["cost_write"])
    crow("output", sa["cost_out"], sb["cost_out"])
    crow("TOTAL", sa["cost_total"], sb["cost_total"], boldit=True)

    out.p(bold(" activity"))
    trow("requests", sa["n_req"], sb["n_req"])
    trow("tool calls", sa["n_tools"], sb["n_tools"])

    # Tool-frequency diff
    names = sorted(set(sa["tool_freq"]) | set(sb["tool_freq"]),
                   key=lambda n: -(sa["tool_freq"].get(n, 0)
                                   + sb["tool_freq"].get(n, 0)))
    if names:
        out.p(bold(" tool-call frequency"))
        tw = min(40, max([LW] + [len(n) for n in names]))
        for n in names:
            a = sa["tool_freq"].get(n, 0)
            b = sb["tool_freq"].get(n, 0)
            d = b - a
            label = n if len(n) <= tw else n[: tw - 1] + "…"
            line = (f"  {label:<{tw}}{a:>{VW}}{b:>{VW}}"
                    f"{('%+d' % d):>{DW}}{'':>{PW}}")
            out.p(_color_dir(line, d))

    if sa["unpriced"] or sb["unpriced"]:
        out.p(yellow("  (some requests were unpriced; excluded from cost rows)"))
    out.p("")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Claude Code token & cost auditor (Copilot-style debug log).")
    sel = ap.add_mutually_exclusive_group()
    sel.add_argument("--latest", action="store_true",
                     help="analyze the most recently modified session (default)")
    sel.add_argument("--session", metavar="UUID",
                     help="analyze session by uuid (prefix ok)")
    sel.add_argument("--file", metavar="PATH",
                     help="analyze an explicit transcript file")
    ap.add_argument("--project", default=DEFAULT_PROJECT_SLUG,
                    help=f"project slug (default: {DEFAULT_PROJECT_SLUG})")
    ap.add_argument("--projects-root", default=str(DEFAULT_PROJECTS_ROOT),
                    help="Claude Code projects root dir")
    ap.add_argument("--list", action="store_true",
                    help="list all sessions in the project with summary cost")
    ap.add_argument("--compare", nargs=2, metavar=("A", "B"),
                    help="compare two sessions (uuid prefix or file path) "
                         "side by side with deltas")
    ap.add_argument("--refresh", action="store_true",
                    help="force re-fetch of live pricing (ignore 24h cache)")
    ap.add_argument("--markdown", metavar="OUT.md",
                    help="also write the report as markdown to this path")
    ap.add_argument("--verbose", action="store_true",
                    help="show fuller tool-call argument summaries")
    args = ap.parse_args(argv)

    # Windows consoles default to cp1252, which can't encode Δ/…/— etc.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    litellm = fetch_pricing(refresh=args.refresh)
    overrides = load_overrides()
    pricebook = PriceBook(litellm, overrides)

    if args.list:
        render_list(args.project, args.projects_root, pricebook)
        return 0

    if args.compare:
        path_a = resolve_ident(args.compare[0], args.project, args.projects_root)
        path_b = resolve_ident(args.compare[1], args.project, args.projects_root)
        sa = summarize(load_session(path_a), pricebook)
        sb = summarize(load_session(path_b), pricebook)
        out = Out(md_path=args.markdown)
        render_compare(out, path_a, sa, path_b, sb)
        out.flush()
        if args.markdown:
            print(dim(f"[markdown written to {args.markdown}]"), file=sys.stderr)
        return 0

    path = resolve_session_file(args)
    requests = load_session(path)
    if not requests:
        print(yellow(f"no priced assistant messages found in {path}"))
        return 0

    out = Out(md_path=args.markdown)
    render_report(out, path, requests, pricebook, verbose=args.verbose)
    out.flush()
    if args.markdown:
        print(dim(f"[markdown written to {args.markdown}]"), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
