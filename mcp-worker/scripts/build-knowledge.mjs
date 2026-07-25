#!/usr/bin/env node
/**
 * build-knowledge.mjs
 *
 * Builds the retrieval knowledge base shipped inside the qail MCP Worker.
 *
 * Reads the qail.rs repo (default: ../.. from mcp-worker/, override with QAIL_RS_PATH)
 * and writes into mcp-worker/src/knowledge/:
 *
 *   corpus.json  - array of retrievable chunks
 *   index.json   - BM25 postings over those chunks
 *   VERSION.json - provenance stamp
 *
 * No dependencies beyond node builtins. Any failure throws and exits non-zero.
 */

import { readFileSync, writeFileSync, mkdirSync, statSync, readdirSync, existsSync } from "node:fs";
import { join, relative, dirname, basename, extname, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));

const REPO = process.env.QAIL_RS_PATH
  ? process.env.QAIL_RS_PATH
  : join(__dirname, "..", "..");
const OUT_DIR = join(__dirname, "..", "src", "knowledge");

const MAX_FILE_BYTES = 256 * 1024;
const TARGET_TOKENS = 800;
const MIN_MERGE_TOKENS = 150;
const HARD_MAX_TOKENS = 1200;

const skipped = [];
const log = (...a) => console.log(...a);

// ---------------------------------------------------------------- utilities

function estTokens(s) {
  return Math.ceil(s.length / 4);
}

function readTextFile(absPath, relPath) {
  const st = statSync(absPath);
  if (st.size > MAX_FILE_BYTES) {
    skipped.push(`${relPath} (${st.size} B > ${MAX_FILE_BYTES} B)`);
    return null;
  }
  const buf = readFileSync(absPath);
  // binary guard: a NUL byte in the first 8KB means this is not text
  if (buf.subarray(0, 8192).includes(0)) {
    skipped.push(`${relPath} (binary: NUL byte)`);
    return null;
  }
  return buf.toString("utf8");
}

function walk(absDir, filterFn, out = []) {
  if (!existsSync(absDir)) return out;
  for (const entry of readdirSync(absDir, { withFileTypes: true })) {
    const abs = join(absDir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "node_modules" || entry.name === "target" || entry.name.startsWith(".")) continue;
      walk(abs, filterFn, out);
    } else if (entry.isFile()) {
      const rel = relative(REPO, abs).split(sep).join("/");
      if (filterFn(rel)) out.push({ abs, rel });
    }
  }
  return out;
}

function slugify(s) {
  return String(s)
    .toLowerCase()
    .replace(/[`*_~]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 60) || "section";
}

/**
 * Detect which schema dialect a chunk teaches, if any.
 *
 * Line-anchored on purpose. A QAIL schema declaration always begins a line
 * (`table users {`). Matching anywhere in the line produced false positives on
 * English prose ("typed table reference (no strings!)") and on raw SQL DDL
 * ("create table tasks ("), which is not the legacy paren schema dialect at all.
 */
function detectDialect(text) {
  if (/^[ \t]*table\s+[A-Za-z_]\w*\s*\{/m.test(text)) return "brace";
  if (/^[ \t]*table\s+[A-Za-z_]\w*\s*\(/m.test(text)) return "paren";
  return "n/a";
}

/** Map a repo-relative path to a subject area. */
function areaFor(rel) {
  if (rel.startsWith("sdk/")) return "sdk";
  if (rel.startsWith("docs/src/core/")) return "core";
  if (rel.startsWith("docs/src/gateway")) return "gateway";
  if (rel.startsWith("docs/src/features/")) return "core";
  if (rel.startsWith("docs/src/reference/cli")) return "cli";
  if (rel.startsWith("core/")) return "core";
  if (rel.startsWith("pg/")) return "pg";
  if (rel.startsWith("gateway/")) return "gateway";
  if (rel.startsWith("cli/")) return "cli";
  if (rel.startsWith("workflow")) return "workflow";
  if (rel.startsWith("qdrant/")) return "qdrant";
  if (rel.startsWith("encoder/")) return "encoder";
  if (rel.startsWith("lsp/")) return "lsp";
  if (rel.startsWith("mcp/")) return "mcp";
  if (rel.startsWith("examples/schema")) return "core";
  return "docs";
}

// ------------------------------------------------------- markdown chunking

/**
 * Split markdown into fence-aware blocks. A fenced code block is always
 * emitted as one indivisible block, so we can never split inside one.
 */
function markdownBlocks(lines) {
  const blocks = [];
  let cur = [];
  let fence = null;
  const flush = () => {
    if (cur.length) blocks.push({ text: cur.join("\n"), atomic: false });
    cur = [];
  };
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const m = line.match(/^(\s{0,3})(`{3,}|~{3,})(.*)$/);
    if (fence === null && m) {
      flush();
      fence = m[2][0].repeat(3);
      cur.push(line);
      continue;
    }
    if (fence !== null) {
      cur.push(line);
      if (m && m[2][0] === fence[0] && m[3].trim() === "") {
        blocks.push({ text: cur.join("\n"), atomic: true });
        cur = [];
        fence = null;
      }
      continue;
    }
    if (line.trim() === "") {
      flush();
      continue;
    }
    cur.push(line);
  }
  if (fence !== null) {
    // unterminated fence: keep it whole rather than risk splitting inside it
    blocks.push({ text: cur.join("\n"), atomic: true });
  } else {
    flush();
  }
  return blocks;
}

/** Parse a markdown doc into heading-delimited sections (H1/H2/H3 boundaries). */
function markdownSections(text, fallbackTitle) {
  const lines = text.split(/\r?\n/);
  const sections = [];
  let fence = null;
  const stack = []; // [{level, title}]
  let cur = { headingPath: [fallbackTitle], title: fallbackTitle, level: 0, lines: [] };

  for (const line of lines) {
    const fm = line.match(/^(\s{0,3})(`{3,}|~{3,})/);
    if (fm) {
      if (fence === null) fence = fm[2][0];
      else if (fm[2][0] === fence) fence = null;
      cur.lines.push(line);
      continue;
    }
    const hm = fence === null ? line.match(/^(#{1,3})\s+(.*\S)\s*$/) : null;
    if (hm) {
      const level = hm[1].length;
      const title = hm[2].replace(/[`*_]/g, "").trim();
      if (cur.lines.join("").trim() !== "" || sections.length === 0) sections.push(cur);
      while (stack.length && stack[stack.length - 1].level >= level) stack.pop();
      stack.push({ level, title });
      const headingPath = stack.map((s) => s.title);
      cur = { headingPath, title, level, lines: [] };
      continue;
    }
    cur.lines.push(line);
  }
  if (cur.lines.join("").trim() !== "") sections.push(cur);

  return sections
    .map((s) => ({ ...s, body: s.lines.join("\n").trim() }))
    .filter((s) => s.body !== "" || s.headingPath.length > 1);
}

/** Turn one markdown file into chunks. */
function chunkMarkdown({ rel, text, kind, weight, idPrefix, idBase }) {
  const fallbackTitle = (text.match(/^#\s+(.*\S)\s*$/m)?.[1] || basename(rel, ".md"))
    .replace(/[`*_]/g, "")
    .trim();
  const sections = markdownSections(text, fallbackTitle);

  // Merge runs of tiny sections so we land nearer the 400-800 token target.
  const merged = [];
  for (const s of sections) {
    const prev = merged[merged.length - 1];
    const sTok = estTokens(s.body);
    if (prev) {
      const prevTok = estTokens(prev.body);
      if (
        (prevTok < MIN_MERGE_TOKENS || sTok < MIN_MERGE_TOKENS) &&
        prevTok + sTok < TARGET_TOKENS
      ) {
        prev.body = `${prev.body}\n\n${s.headingPath.length ? "#".repeat(Math.max(s.level, 2)) + " " + s.title + "\n\n" : ""}${s.body}`.trim();
        continue;
      }
    }
    merged.push({ ...s });
  }

  const chunks = [];
  for (const s of merged) {
    const headingPath = s.headingPath.filter(Boolean);
    const prefix = headingPath.join(" > ");
    const pieces = [];

    if (estTokens(s.body) <= HARD_MAX_TOKENS) {
      pieces.push(s.body);
    } else {
      // Overflow: repack fence-aware blocks up to TARGET_TOKENS.
      const blocks = markdownBlocks(s.body.split(/\r?\n/));
      let acc = [];
      let accTok = 0;
      for (const b of blocks) {
        const bTok = estTokens(b.text);
        if (acc.length && accTok + bTok > TARGET_TOKENS) {
          pieces.push(acc.join("\n\n"));
          acc = [];
          accTok = 0;
        }
        acc.push(b.text);
        accTok += bTok;
      }
      if (acc.length) pieces.push(acc.join("\n\n"));
    }

    pieces.forEach((body, i) => {
      if (!body.trim()) return;
      const content = `${prefix}\n\n${body}`.trim();
      const slug = slugify(s.title) + (pieces.length > 1 ? `-${i + 1}` : "");
      chunks.push({
        id: `${idPrefix}/${idBase}#${slug}`,
        path: rel,
        kind,
        area: areaFor(rel),
        title: s.title,
        headingPath,
        content,
        tokens: estTokens(content),
        weight,
        dialect: detectDialect(content),
      });
    });
  }
  return chunks;
}

/** Turn one Rust example file into chunks, splitting only at `fn ` boundaries. */
function chunkRust({ rel, text }) {
  const title = basename(rel, ".rs");
  const mk = (body, slug, fnName) => {
    const content = `${rel}\n\n${body}`.trim();
    const tokens = estTokens(content);
    const c = {
      id: `example-code/${title}#${slug}`,
      path: rel,
      kind: "example-code",
      area: areaFor(rel),
      title: fnName ? `${title}: ${fnName}` : title,
      headingPath: fnName ? [title, fnName] : [title],
      content,
      tokens,
      weight: 1.0,
      dialect: detectDialect(body),
    };
    // A single fn body can legitimately exceed the hard max: the chunking
    // contract forbids splitting mid-fn. Flag it so retrieval can budget for it
    // rather than being surprised by an oversized chunk.
    if (tokens > HARD_MAX_TOKENS) c.atomic = true;
    return c;
  };

  if (estTokens(text) <= HARD_MAX_TOKENS) return [mk(text, "all", null)];

  const lines = text.split(/\r?\n/);
  const starts = [];
  for (let i = 0; i < lines.length; i++) {
    // top-level-ish fn declarations only; never mid-expression
    const m = lines[i].match(/^\s{0,4}(?:pub\s+)?(?:async\s+)?fn\s+([A-Za-z_]\w*)/);
    if (m) starts.push({ line: i, name: m[1] });
  }
  if (starts.length === 0) return [mk(text, "all", null)];

  const chunks = [];
  const preamble = lines.slice(0, starts[0].line).join("\n").trim();
  if (preamble) chunks.push(mk(preamble, "preamble", null));

  for (let s = 0; s < starts.length; s++) {
    const from = starts[s].line;
    const to = s + 1 < starts.length ? starts[s + 1].line : lines.length;
    const body = lines.slice(from, to).join("\n").trim();
    if (!body) continue;
    chunks.push(mk(body, slugify(starts[s].name) + `-${s + 1}`, starts[s].name));
  }
  return chunks;
}

// ---------------------------------------------------------------- collection

const corpus = [];
const addAll = (cs) => cs.forEach((c) => corpus.push(c));

// 1. mdbook pages: docs/src/**  (exclude changelog.md)
{
  const files = walk(join(REPO, "docs", "src"), (rel) => rel.endsWith(".md"))
    .filter(({ rel }) => rel !== "docs/src/changelog.md")
    .sort((a, b) => a.rel.localeCompare(b.rel));
  if (files.length === 0) throw new Error("no mdbook pages found under docs/src");
  for (const { abs, rel } of files) {
    const text = readTextFile(abs, rel);
    if (text === null) continue;
    const idBase = rel.replace(/^docs\/src\//, "").replace(/\.md$/, "");
    addAll(chunkMarkdown({ rel, text, kind: "guide", weight: 2.0, idPrefix: "guide", idBase }));
  }
}

// 2. top-level guides: docs/*.md  (docs/internal/** never walked)
{
  const dir = join(REPO, "docs");
  const files = readdirSync(dir, { withFileTypes: true })
    .filter((e) => e.isFile() && e.name.endsWith(".md"))
    .map((e) => ({ abs: join(dir, e.name), rel: `docs/${e.name}` }))
    .sort((a, b) => a.rel.localeCompare(b.rel));
  for (const { abs, rel } of files) {
    const text = readTextFile(abs, rel);
    if (text === null) continue;
    const idBase = basename(rel, ".md");
    addAll(chunkMarkdown({ rel, text, kind: "guide", weight: 2.0, idPrefix: "guide", idBase }));
  }
}

// 3. crate READMEs
{
  const crates = ["core", "pg", "gateway", "qdrant", "workflow", "workflow-postgres", "encoder", "cli", "lsp", "mcp"];
  for (const c of crates) {
    const rel = `${c}/README.md`;
    const abs = join(REPO, rel);
    if (!existsSync(abs)) throw new Error(`expected crate README missing: ${rel}`);
    const text = readTextFile(abs, rel);
    if (text === null) continue;
    addAll(chunkMarkdown({ rel, text, kind: "crate-readme", weight: 1.3, idPrefix: "crate", idBase: c }));
  }
}

// 4. SDK READMEs (only dart + typescript exist; swift/kotlin have none)
{
  for (const s of ["dart", "typescript", "swift", "kotlin"]) {
    const rel = `sdk/${s}/README.md`;
    const abs = join(REPO, rel);
    if (!existsSync(abs)) continue;
    const text = readTextFile(abs, rel);
    if (text === null) continue;
    addAll(chunkMarkdown({ rel, text, kind: "sdk-readme", weight: 1.3, idPrefix: "sdk", idBase: s }));
  }
}

// 5. schema samples: examples/schema/** — whole file, never split
{
  const files = walk(join(REPO, "examples", "schema"), (rel) => rel.endsWith(".qail") || rel.endsWith(".md"))
    .sort((a, b) => a.rel.localeCompare(b.rel));
  if (files.length === 0) throw new Error("no schema samples found under examples/schema");
  for (const { abs, rel } of files) {
    const text = readTextFile(abs, rel);
    if (text === null) continue;
    if (rel.endsWith(".md")) {
      addAll(chunkMarkdown({ rel, text, kind: "schema-sample", weight: 3.0, idPrefix: "schema", idBase: "readme" }));
      continue;
    }
    const idBase = rel.replace(/^examples\/schema\//, "").replace(/\.qail$/, "");
    const content = `Schema sample: ${rel}\n\n${text.trim()}`;
    corpus.push({
      id: `schema/${idBase}`,
      path: rel,
      kind: "schema-sample",
      area: "core",
      title: `Schema sample: ${idBase}`,
      headingPath: ["Schema samples", idBase],
      content,
      tokens: estTokens(content),
      weight: 3.0,
      dialect: detectDialect(text),
    });
  }
}

// 6. core/examples/*.rs  (pg/examples excluded entirely)
{
  const dir = join(REPO, "core", "examples");
  const files = readdirSync(dir, { withFileTypes: true })
    .filter((e) => e.isFile() && e.name.endsWith(".rs"))
    .map((e) => ({ abs: join(dir, e.name), rel: `core/examples/${e.name}` }))
    .sort((a, b) => a.rel.localeCompare(b.rel));
  if (files.length === 0) throw new Error("no rust examples found under core/examples");
  for (const { abs, rel } of files) {
    const text = readTextFile(abs, rel);
    if (text === null) continue;
    addAll(chunkRust({ rel, text }));
  }
}

// 7. docs/generated/*.json from knowledge_export
{
  const gdir = join(REPO, "docs", "generated");
  const readJson = (name) => {
    const abs = join(gdir, name);
    if (!existsSync(abs)) throw new Error(`missing generated knowledge file: docs/generated/${name} (run: cargo run -p qail-core --example knowledge_export)`);
    return JSON.parse(readFileSync(abs, "utf8"));
  };

  // 7a. grammar productions
  const productions = readJson("grammar-productions.json");
  if (!Array.isArray(productions) || productions.length === 0) throw new Error("grammar-productions.json is empty");
  for (const p of productions) {
    const parts = [`Grammar production: ${p.production}`, `Construct: ${p.construct}`, `Source: ${p.file}`];
    if (p.doc) parts.push(`\n${p.doc}`);
    if (p.example) parts.push(`\nExample:\n${p.example}`);
    if (Array.isArray(p.keywords) && p.keywords.length) parts.push(`\nKeywords: ${p.keywords.join(", ")}`);
    if (Array.isArray(p.calls) && p.calls.length) parts.push(`Calls: ${p.calls.join(", ")}`);
    const content = parts.join("\n");
    corpus.push({
      id: `production/${p.construct}/${p.production}`,
      path: p.file,
      kind: "production",
      area: areaFor(p.file || "core/"),
      title: `${p.construct}: ${p.production}`,
      headingPath: ["Grammar", p.construct, p.production],
      content,
      tokens: estTokens(content),
      weight: 3.0,
      dialect: detectDialect(content),
    });
  }

  // 7b. verified examples — every one actually ran through parser + transpiler
  const verified = readJson("verified-examples.json");
  if (!Array.isArray(verified) || verified.length === 0) throw new Error("verified-examples.json is empty");
  verified.forEach((e, i) => {
    const content = `${e.input}\n-- SQL:\n${e.sql}`;
    corpus.push({
      id: `example/${i}`,
      path: e.source,
      kind: "example",
      area: "core",
      title: `${e.action}${e.table ? " " + e.table : ""}: ${e.input.slice(0, 60)}`,
      headingPath: ["Verified examples", e.action || "query"],
      content,
      tokens: estTokens(content),
      weight: 3.0,
      dialect: "n/a",
    });
  });

  // 7c. invalid examples — what NOT to generate
  const invalid = readJson("invalid-examples.json");
  if (!Array.isArray(invalid) || invalid.length === 0) throw new Error("invalid-examples.json is empty");
  invalid.forEach((e, i) => {
    const content = `INVALID QAIL (do not generate):\n${e.input}\n-- Why:\n${e.error}`;
    corpus.push({
      id: `negative/${i}`,
      path: e.source,
      kind: "negative",
      area: "core",
      title: `Invalid: ${e.input.slice(0, 60)}`,
      headingPath: ["Invalid examples", `case ${i + 1}`],
      content,
      tokens: estTokens(content),
      weight: 1.5,
      dialect: "n/a",
    });
  });
}

// ------------------------------------------------------------- id uniqueness

{
  const seen = new Map();
  for (const c of corpus) {
    if (!seen.has(c.id)) {
      seen.set(c.id, 1);
      continue;
    }
    const n = seen.get(c.id) + 1;
    seen.set(c.id, n);
    c.id = `${c.id}~${n}`;
  }
  const ids = new Set(corpus.map((c) => c.id));
  if (ids.size !== corpus.length) throw new Error("chunk ids are not unique after disambiguation");
}

// ----------------------------------------------------------------- BM25 index

const STOPWORDS = new Set(
  ("a an and are as at be but by for from has have he her his if in into is it its of on or " +
    "our she that the their then there these they this to was were will with you your we us " +
    "can could should would do does did not no so such than them when where which who whom why " +
    "how what while about above after again all also am any because been before being below " +
    "between both during each few further here him himself i itself just me more most my myself " +
    "nor now off once only other out over own same some through too under until up very via").split(/\s+/)
);

/**
 * Returns { terms, length }.
 *
 * `terms` carries a light plural stem alongside each original token, so a query
 * for "field" still hits a chunk that only ever writes "fields". That makes the
 * vocabulary a superset of the text.
 *
 * `length` counts ONLY the original tokens. The stems are index padding, not
 * words the document actually contains — counting them would inflate every
 * chunk's length and skew BM25's length normalisation against plural-heavy
 * chunks. knowledge.ts documents this same split; keep the two in agreement.
 */
function tokenize(s) {
  const raw = s.toLowerCase().split(/[^a-z0-9]+/);
  const terms = [];
  let length = 0;
  for (const t of raw) {
    if (t.length < 2) continue;
    if (STOPWORDS.has(t)) continue;
    terms.push(t);
    length++;
    // light stem: strip a trailing plural 's'
    if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss")) {
      const stem = t.slice(0, -1);
      if (stem.length >= 2 && !STOPWORDS.has(stem)) terms.push(stem);
    }
  }
  return { terms, length };
}

const df = Object.create(null);
const postings = Object.create(null);
const len = [];

corpus.forEach((c, idx) => {
  // title + headingPath are part of the searchable surface
  const { terms, length } = tokenize(`${c.title} ${c.headingPath.join(" ")} ${c.content}`);
  len.push(length);
  const tf = new Map();
  for (const t of terms) tf.set(t, (tf.get(t) || 0) + 1);
  for (const [term, freq] of tf) {
    df[term] = (df[term] || 0) + 1;
    (postings[term] || (postings[term] = [])).push([idx, freq]);
  }
});

const totalIndexedTerms = len.reduce((a, b) => a + b, 0);
const index = {
  df,
  postings,
  len,
  avgLen: corpus.length ? totalIndexedTerms / corpus.length : 0,
  n: corpus.length,
};

// -------------------------------------------------------------------- write

mkdirSync(OUT_DIR, { recursive: true });

const corpusPath = join(OUT_DIR, "corpus.json");
const indexPath = join(OUT_DIR, "index.json");
const versionPath = join(OUT_DIR, "VERSION.json");

const corpusJson = JSON.stringify(corpus);
const indexJson = JSON.stringify(index);
writeFileSync(corpusPath, corpusJson);
writeFileSync(indexPath, indexJson);

let qailCommit = "unknown";
try {
  qailCommit = execFileSync("git", ["-C", REPO, "rev-parse", "--short", "HEAD"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  }).trim();
} catch {
  throw new Error(`could not read git commit from ${REPO}`);
}

const coreCargo = readFileSync(join(REPO, "core", "Cargo.toml"), "utf8");
const qailVersion = coreCargo.match(/^\s*version\s*=\s*"([^"]+)"/m)?.[1];
if (!qailVersion) throw new Error("could not read version from core/Cargo.toml");

// Deterministic on purpose: every field here is a pure function of the checked
// out tree, so re-running the builder on an unchanged repo reproduces all three
// outputs byte for byte. That is what lets CI gate on `git diff --exit-code`.
//
// There is deliberately no `generatedAt` timestamp: nothing reads it, and a
// wall-clock field would make the drift gate fail on every run. `qailCommit` is
// the one field that legitimately moves without the corpus changing (committing
// a regenerated VERSION.json advances HEAD), so the CI gate compares VERSION.json
// with that key removed. See .github/workflows/mcp.yml, job `corpus-fresh`.
writeFileSync(
  versionPath,
  JSON.stringify({ qailCommit, qailVersion, chunkCount: corpus.length }, null, 2) + "\n"
);

// --------------------------------------------------------------- assertions

const byKind = {};
for (const c of corpus) byKind[c.kind] = (byKind[c.kind] || 0) + 1;

const REQUIRED_KINDS = ["guide", "crate-readme", "sdk-readme", "example-code", "schema-sample", "production", "example", "negative"];
const errors = [];

if (corpus.length < 300 || corpus.length > 2000) {
  errors.push(`chunk count ${corpus.length} outside [300, 2000]`);
}
for (const k of REQUIRED_KINDS) {
  if (!byKind[k]) errors.push(`no chunks of kind "${k}"`);
}
const totalBytes = Buffer.byteLength(corpusJson) + Buffer.byteLength(indexJson);
if (totalBytes > 3 * 1024 * 1024) {
  errors.push(`corpus.json + index.json = ${totalBytes} B exceeds 3 MB`);
}
// Enforce the hard max on every chunk that was splittable. Chunks marked
// `atomic` are single fn bodies the contract forbids splitting, so they are
// exempt from the error but still surfaced in the summary below.
const oversize = corpus.filter((c) => c.tokens > HARD_MAX_TOKENS && !c.atomic);
if (oversize.length) {
  errors.push(`${oversize.length} splittable chunk(s) exceed ${HARD_MAX_TOKENS} tokens, e.g. ${oversize[0].id} (${oversize[0].tokens})`);
}
const atomicOversize = corpus.filter((c) => c.atomic);

// ------------------------------------------------------------------ summary

const totalTokens = corpus.reduce((a, c) => a + c.tokens, 0);
const kb = (b) => `${(b / 1024).toFixed(1)} KB`;

log("");
log("qail knowledge base");
log(`  repo            ${REPO}`);
log(`  commit          ${qailCommit}  (qail-core ${qailVersion})`);
log(`  chunks          ${corpus.length}`);
log(`  total tokens    ${totalTokens.toLocaleString("en-US")}`);
log(`  indexed terms   ${totalIndexedTerms.toLocaleString("en-US")} (${Object.keys(df).length.toLocaleString("en-US")} unique, avgLen ${index.avgLen.toFixed(1)})`);
log("");
log("  outputs");
log(`    corpus.json   ${kb(Buffer.byteLength(corpusJson))}`);
log(`    index.json    ${kb(Buffer.byteLength(indexJson))}`);
log(`    combined      ${kb(totalBytes)} / 3072.0 KB budget`);
log("");
log("  by kind");
for (const k of Object.keys(byKind).sort((a, b) => byKind[b] - byKind[a])) {
  const toks = corpus.filter((c) => c.kind === k).reduce((a, c) => a + c.tokens, 0);
  log(`    ${k.padEnd(14)} ${String(byKind[k]).padStart(4)} chunks  ${String(toks).padStart(7)} tokens`);
}

if (atomicOversize.length) {
  log("");
  log(`  oversize but indivisible (single fn body, never split mid-fn)`);
  for (const c of atomicOversize.sort((a, b) => b.tokens - a.tokens)) {
    log(`    ${String(c.tokens).padStart(5)} tokens  ${c.id}`);
  }
}

if (skipped.length) {
  log("");
  log("  skipped files");
  for (const s of skipped) log(`    ${s}`);
}

if (errors.length) {
  log("");
  console.error("ASSERTIONS FAILED:");
  for (const e of errors) console.error(`  - ${e}`);
  process.exit(1);
}

log("");
log("  assertions      OK");
log("");
