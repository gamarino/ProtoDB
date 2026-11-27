"""The Markdown tutorials under docs/tutorial run, and their scripts are their code (README section "Tutorial").

Three tutorials live there, written in Markdown so that they render where readers of a release land (GitHub):
the complete tutorial (``docs/tutorial``), the queries tutorial (``docs/tutorial/queries``) and the semantic
model tutorial (``docs/tutorial/semantic-model``). Each chapter ``NN-<slug>.md`` that has code has a script,
``examples/tutorial[/queries|/semantic]/NN_<slug>.py``, generated from it.

Catches: a chapter that no longer runs after an API change; an ``Output`` that is not what its code printed; a
chapter shown with code that never runs (only ``python`` blocks run, and only ``python`` and ``text`` blocks are
allowed); a script that drifted from its chapter (it must equal the generated text); a chapter missing from its
tutorial's index; and a relative link, in the tutorials or in README.md, to a file that does not exist.

How a chapter is read:

* its ```` ```python ```` blocks run in order, in one namespace, like the cells of a notebook, in a fresh
  interpreter and a fresh working directory, with the repository and ``examples/tutorial`` (the tutorials'
  shared modules) on ``sys.path``, exactly as the generated script runs them;
* a ```` ```text ```` block whose preceding paragraph is ``Output:`` must equal, line for line, what the
  python blocks since the previous ``Output`` printed;
* a ``text`` block whose preceding paragraph starts with ``Sample output`` shows one run of timing-dependent
  code and is not compared (the code asserts the property it demonstrates instead); like an ``Output``, it
  ends what the next ``Output`` is compared with;
* any other ``text`` block is prose (a diagram, a command, a file layout).

The generated script is the chapter's python blocks, verbatim and in order, with a call after the blocks of each
``Output`` that compares what they printed with it, and a call after each ``Sample output`` that drops what it
shows: the script checks every figure the chapter shows, exits with status 1 on the first difference, and
prints ``<script name>: OK`` last. ``tests/integration/test_examples.py``
runs the scripts as a user would.

Maintenance (run from the repository root)::

    python tests/docs/test_markdown_tutorials.py --show docs/tutorial/05-concurrency.md    # what each block prints
    python tests/docs/test_markdown_tutorials.py --update docs/tutorial/05-concurrency.md  # rewrite its Outputs
    python tests/docs/test_markdown_tutorials.py --write-scripts                           # regenerate every script

After ``--update``, review the prose that quotes a figure, then regenerate the scripts.
"""
import contextlib
import io
import os
import re
import subprocess
import sys
import tempfile
import traceback

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SHARED = os.path.join(REPO, "examples", "tutorial")

#: Each tutorial directory (relative to the repository) and the directory of its scripts.
TUTORIALS = {
    os.path.join("docs", "tutorial"): os.path.join("examples", "tutorial"),
    os.path.join("docs", "tutorial", "queries"): os.path.join("examples", "tutorial", "queries"),
    os.path.join("docs", "tutorial", "semantic-model"): os.path.join("examples", "tutorial", "semantic"),
}
CHAPTER = re.compile(r"^(\d\d)-([a-z0-9-]+)\.md$")
FENCE_OPEN = re.compile(r"^```(\S*)\s*$")
FENCE_CLOSE = re.compile(r"^```\s*$")


def chapters() -> list:
    """Every chapter file, relative to the repository, in tutorial then name order."""
    out = []
    for directory in TUTORIALS:
        names = sorted(n for n in os.listdir(os.path.join(REPO, directory)) if CHAPTER.match(n))
        out += [os.path.join(directory, n) for n in names]
    return out


def script_for(chapter: str) -> str:
    """The script generated from ``chapter`` (both relative to the repository)."""
    directory, name = os.path.split(chapter)
    number, slug = CHAPTER.match(name).groups()
    return os.path.join(TUTORIALS[directory], f"{number}_{slug.replace('-', '_')}.py")


def parse_blocks(text: str) -> list:
    """The fenced blocks of a Markdown text as ``(line_number, language, caption, source)`` tuples.

    ``caption`` is the first line of the paragraph just before the fence (``Output:``, ``Sample output ...``,
    which may continue on the next lines) or ``None``."""
    lines = text.splitlines()
    blocks, i = [], 0
    while i < len(lines):
        match = FENCE_OPEN.match(lines[i])
        if not match or not match.group(1):
            if match:
                raise AssertionError(f"line {i + 1}: a fence without a language")
            i += 1
            continue
        start, language = i + 1, match.group(1)
        before = i - 1
        while before >= 0 and not lines[before].strip():
            before -= 1
        while before > 0 and lines[before - 1].strip() and not FENCE_CLOSE.match(lines[before - 1]):
            before -= 1                                   # back to the first line of that paragraph
        previous = lines[before].strip() if before >= 0 else ""
        caption = previous if previous == "Output:" or previous.startswith("Sample output") else None
        body = []
        i += 1
        while i < len(lines) and not FENCE_CLOSE.match(lines[i]):
            body.append(lines[i])
            i += 1
        if i == len(lines):
            raise AssertionError(f"line {start}: the block is never closed")
        blocks.append((start, language, caption, "\n".join(body)))
        i += 1
    return blocks


def normalise(text: str) -> str:
    """Compare outputs without trailing whitespace or blank edges."""
    return "\n".join(line.rstrip() for line in text.strip().splitlines())


def check_languages(path: str, blocks: list) -> None:
    for line, language, caption, _ in blocks:
        where = f"{path}:{line}"
        assert language in ("python", "text"), \
            f"{where}: language {language!r}: a chapter shows only python (run) or text (output or prose)"
        if caption == "Output:":
            assert language == "text", f"{where}: an Output is a text block"


def run_chapter(path: str, report=None, update: bool = False) -> int:
    """Run the python blocks of the chapter at ``path`` in one namespace; return how many ran.

    ``report(line, output)`` is called after each block when given. With ``update``, Output blocks that differ
    are rewritten with what the code printed."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    blocks = parse_blocks(text)
    check_languages(path, blocks)
    namespace = {"__name__": "__main__"}
    printed, ran, replacements = "", 0, []
    for line, language, caption, source in blocks:
        where = f"{os.path.basename(path)}:{line}"
        if language == "python":
            buffer = io.StringIO()
            try:
                with contextlib.redirect_stdout(buffer):
                    exec(compile(source, where, "exec"), namespace)
            except BaseException:
                raise AssertionError(f"{where}: this block failed\n{traceback.format_exc()}\n"
                                     f"--- output so far ---\n{buffer.getvalue()}") from None
            printed += buffer.getvalue()
            ran += 1
            if report:
                report(line, buffer.getvalue())
        elif caption == "Output:":
            if normalise(source) != normalise(printed):
                assert update, (f"{where}: the Output differs from what the code printed\n"
                                f"--- shown ---\n{normalise(source)}\n--- printed ---\n{normalise(printed)}")
                replacements.append((line, len(source.splitlines()), normalise(printed)))
            printed = ""
        elif caption is not None:                  # a Sample output shows what was printed since the last one
            printed = ""
    if replacements:
        lines = text.splitlines()
        for start, count, body in sorted(replacements, reverse=True):
            lines[start:start + count] = body.splitlines()
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    return ran


def _literal(text: str) -> str:
    """``text`` as a readable Python string literal."""
    if '"""' not in text and "\\" not in text and not text.endswith('"'):
        return '"""\n' + text + '\n"""'
    return repr("\n" + text + "\n")


def generate_script(chapter: str) -> str:
    """The script of ``chapter`` (relative to the repository): its python blocks, each Output checked."""
    with open(os.path.join(REPO, chapter), encoding="utf-8") as f:
        text = f.read()
    blocks = parse_blocks(text)
    title = next(line[2:].strip() for line in text.splitlines() if line.startswith("# "))
    script = script_for(chapter)
    stem = os.path.basename(script)[:-3]
    up = script.count(os.sep)                      # examples/tutorial/x.py: 2 directories below the repository
    head = f'''#!/usr/bin/env python3
"""{title}

Generated from {chapter.replace(os.sep, "/")} by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "{stem}: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python {script.replace(os.sep, "/")}
"""
import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_REPO = _os.path.normpath(_os.path.join(_HERE, *[_os.pardir] * {up}))
_sys.path[:0] = [_REPO, _os.path.join(_REPO, "examples", "tutorial")]


class _Tee:
    """Standard output that also keeps what was printed since the last check."""

    def __init__(self, stream):
        self.stream, self.kept = stream, []

    def write(self, text):
        self.kept.append(text)
        return self.stream.write(text)

    def flush(self):
        self.stream.flush()


_sys.stdout = _Tee(_sys.stdout)


def _expect(shown):
    """Exit with status 1 unless the code since the last check printed ``shown`` (the chapter's Output)."""
    printed = "".join(_sys.stdout.kept)
    _sys.stdout.kept.clear()
    def normal(text):
        return "\\n".join(line.rstrip() for line in text.strip().splitlines())
    if normal(printed) != normal(shown):
        _sys.stderr.write(f"--- the chapter shows ---\\n{{normal(shown)}}\\n--- the code printed ---\\n{{normal(printed)}}\\n")
        _sys.exit(1)


def _sample():
    """The chapter shows what the code since the last check printed as a sample of one run: not compared."""
    _sys.stdout.kept.clear()

'''
    parts = [head]
    for line, language, caption, source in blocks:
        if language == "python":
            parts.append(f"\n# -- {os.path.basename(chapter)}, line {line} " + "-" * 40 + "\n" + source.rstrip() + "\n")
        elif caption == "Output:":
            parts.append(f"_expect({_literal(normalise(source))})\n")
        elif caption is not None:
            parts.append("_sample()\n")
    parts.append(f'\nprint("{stem}: OK")\n')
    return "".join(parts)


def python_blocks(chapter: str) -> list:
    with open(os.path.join(REPO, chapter), encoding="utf-8") as f:
        return [b for b in parse_blocks(f.read()) if b[1] == "python"]


CODE_CHAPTERS = [c for c in chapters() if python_blocks(c)]


def _child(args, cwd):
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run([sys.executable, *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=300)


@pytest.mark.parametrize("chapter", CODE_CHAPTERS)
@pytest.mark.timeout(330)
def test_chapter_runs(chapter, tmp_path):
    """Every python block of the chapter runs, in a fresh interpreter, and every Output matches its code."""
    out = _child([os.path.abspath(__file__), "--run", os.path.join(REPO, chapter)], str(tmp_path))
    assert out.returncode == 0, out.stdout + out.stderr
    ran = int(out.stdout.split()[-1])
    assert ran >= 2, f"{chapter}: a chapter with code has at least two runnable blocks, not {ran}"


@pytest.mark.parametrize("chapter", CODE_CHAPTERS)
def test_script_is_the_chapter(chapter):
    """The chapter's script is exactly what its chapter generates (regenerate with --write-scripts)."""
    path = os.path.join(REPO, script_for(chapter))
    assert os.path.exists(path), f"{script_for(chapter)} is missing: run --write-scripts"
    with open(path, encoding="utf-8") as f:
        assert f.read() == generate_script(chapter), f"{script_for(chapter)} drifted from {chapter}: run --write-scripts"


def test_every_script_has_its_chapter():
    """No script outlives its chapter, and a chapter without code has no script."""
    expected = {script_for(c) for c in CODE_CHAPTERS}
    found = set()
    for scripts in TUTORIALS.values():
        found |= {os.path.join(scripts, n) for n in os.listdir(os.path.join(REPO, scripts)) if re.match(r"^\d\d_", n)}
    assert found == expected


@pytest.mark.parametrize("directory", sorted(TUTORIALS))
def test_every_chapter_is_in_its_index(directory):
    """Each tutorial's README.md links every chapter of the tutorial."""
    with open(os.path.join(REPO, directory, "README.md"), encoding="utf-8") as f:
        index = f.read()
    names = [n for n in os.listdir(os.path.join(REPO, directory)) if CHAPTER.match(n)]
    assert len(names) >= 5, f"{directory}: a tutorial of {len(names)} chapters"
    for name in names:
        assert f"]({name})" in index, f"{directory}/README.md does not link {name}"


def _markdown_files() -> list:
    found = [os.path.join(REPO, "README.md"), os.path.join(REPO, "examples", "README.md")]
    for directory in TUTORIALS:
        found += [os.path.join(REPO, directory, n) for n in sorted(os.listdir(os.path.join(REPO, directory)))
                  if n.endswith(".md")]
    return found


@pytest.mark.parametrize("path", _markdown_files(), ids=lambda p: os.path.relpath(p, REPO))
def test_relative_links_resolve(path):
    """Every relative link of the tutorials, README.md and examples/README.md names a file that exists."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    targets = re.findall(r"\]\(([^)\s]+)\)", text)
    missing = []
    for target in targets:
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            continue
        file_part = target.split("#")[0]
        if not os.path.exists(os.path.normpath(os.path.join(os.path.dirname(path), file_part))):
            missing.append(target)
    assert not missing, f"{os.path.relpath(path, REPO)} links files that do not exist: {missing}"


def test_the_parser_reads_captions_and_bodies():
    text = "Intro.\n\n```python\nx = 1\nprint(x)\n```\n\nOutput:\n\n```text\n1\n```\n\nSample output (varies):\n\n```text\n2\n```\n"
    assert parse_blocks(text) == [(3, "python", None, "x = 1\nprint(x)"), (10, "text", "Output:", "1"),
                                  (16, "text", "Sample output (varies):", "2")]
    two_lines = "Sample output (one run,\non this machine):\n\n```text\n3\n```\n"
    assert parse_blocks(two_lines) == [(4, "text", "Sample output (one run,", "3")]


def _main(argv: list) -> None:
    sys.path[:0] = [REPO, SHARED]
    if argv[:1] == ["--run"]:
        print(run_chapter(argv[1]))
        return
    if argv[:1] == ["--write-scripts"]:
        for chapter in CODE_CHAPTERS:
            with open(os.path.join(REPO, script_for(chapter)), "w", encoding="utf-8") as f:
                f.write(generate_script(chapter))
            print("wrote", script_for(chapter))
        return
    update = argv[:1] == ["--update"]
    targets = [os.path.abspath(t) for t in argv[1:]]
    os.chdir(tempfile.mkdtemp(prefix="md-tutorial-", dir=os.environ.get("TUTORIAL_TMP")))
    for target in targets:
        def show(line, output):
            print(f"=== block at line {line}\n{output}", end="" if output.endswith("\n") or not output else "\n")
        run_chapter(target, show, update)


if __name__ == "__main__":
    _main(sys.argv[1:])
