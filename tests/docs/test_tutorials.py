"""Every code block of every tutorial in docs/source/tutorials runs (spec section 13, Task 10.2).

Catches: a tutorial that no longer works after an API change, a figure in a tutorial that does not
come from running its code, and a tutorial with a code block that is shown but never executed.

How a tutorial is read:

* the ``.. code-block:: python`` blocks of one tutorial run in order, in one shared namespace, like
  the cells of a notebook, in a fresh working directory (relative paths are safe);
* a ``.. code-block:: text`` block whose caption is ``Output`` must equal, line for line, what the
  block before it printed: the figures a tutorial shows are the figures its code produced;
* a ``text`` block whose caption starts with ``Sample output`` shows one run of timing-dependent
  code; it is not compared (the code asserts the property it demonstrates instead);
* any other ``text`` block is prose (a diagram, a file layout); no other language is allowed, so
  nothing can be shown without being run.

Each tutorial runs once per process: the custom-index tutorial defines atom classes, which register
process-wide under their ``__type_name__``, so running it twice in one process fails.

Run ``python tests/docs/test_tutorials.py <tutorial.rst>`` to print what each block prints, and
``--update`` before the file names to rewrite the ``Output`` blocks with what the code printed (review
the prose that quotes a figure afterwards).
"""
import contextlib
import io
import os
import re
import sys
import threading
import traceback

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)
TUTORIAL_DIR = os.path.join(REPO, "docs", "source", "tutorials")
DIRECTIVE = re.compile(r"^\.\. code-block:: (\S+)\s*$")
INDENT = 3


def tutorial_files():
    """The tutorial pages, in name order (``index.rst`` is the table of contents, not a tutorial)."""
    return sorted(name for name in os.listdir(TUTORIAL_DIR) if name.endswith(".rst") and name != "index.rst")


def parse_blocks(text: str) -> list:
    """The code blocks of an .rst text as ``(line_number, language, caption, source)`` tuples."""
    lines = text.splitlines()
    blocks = []
    i = 0
    while i < len(lines):
        match = DIRECTIVE.match(lines[i])
        if not match:
            i += 1
            continue
        start, language, caption = i + 1, match.group(1), None
        i += 1
        while i < len(lines) and lines[i].startswith(" " * INDENT + ":"):
            option = lines[i].strip()
            if option.startswith(":caption:"):
                caption = option[len(":caption:"):].strip()
            i += 1
        body = []
        while i < len(lines) and (not lines[i].strip() or lines[i].startswith(" " * INDENT)):
            body.append(lines[i][INDENT:] if lines[i].strip() else "")
            i += 1
        blocks.append((start, language, caption, "\n".join(body).strip("\n")))
    return blocks


def normalise(text: str) -> str:
    """Compare outputs without trailing whitespace or blank edges."""
    return "\n".join(line.rstrip() for line in text.strip().splitlines())


def run_tutorial(path: str, report=None, update: bool = False) -> int:
    """Run every python block of ``path`` in one namespace; return the number of blocks run.

    ``report(line, source, output)`` is called after each block when given.
    """
    with open(path, encoding="utf-8") as f:
        text = f.read()
    blocks = parse_blocks(text)
    replacements = []                    # (first line index, last line index, new body lines)
    namespace = {"__name__": "__tutorial__"}
    last_output, ran = None, 0
    for line, language, caption, source in blocks:
        where = f"{os.path.basename(path)}:{line}"
        if language == "python":
            buffer = io.StringIO()
            try:
                with contextlib.redirect_stdout(buffer):
                    exec(compile(source, f"{where}", "exec"), namespace)
            except BaseException:
                raise AssertionError(f"{where}: this block failed\n{traceback.format_exc()}\n"
                                     f"--- output so far ---\n{buffer.getvalue()}") from None
            last_output = buffer.getvalue()
            ran += 1
            if report:
                report(line, source, last_output)
        elif language == "text":
            if caption == "Output":
                assert last_output is not None, f"{where}: an Output block needs a python block before it"
                if update and normalise(source) != normalise(last_output):
                    replacements.append((line, len(source.splitlines()), normalise(last_output).splitlines()))
                    continue
                assert normalise(source) == normalise(last_output), (
                    f"{where}: the Output block differs from what the code printed\n"
                    f"--- shown ---\n{normalise(source)}\n--- printed ---\n{normalise(last_output)}")
            else:
                assert caption is None or caption.startswith("Sample output"), (
                    f"{where}: a text block is prose or has the caption 'Output' / 'Sample output ...'")
        else:
            raise AssertionError(f"{where}: language {language!r}: a tutorial shows only python (run) or text (prose)")
    if update and replacements:
        lines = text.splitlines()
        for start, count, body in sorted(replacements, reverse=True):
            index = start                                  # 0-based index of the first option line
            while lines[index].startswith(" " * INDENT + ":") or not lines[index].strip():
                index += 1                                 # skip the options and the blank line
            lines[index:index + count] = [(" " * INDENT + l) if l else "" for l in body]
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    return ran


@pytest.mark.parametrize("name", tutorial_files())
@pytest.mark.timeout(300)
def test_tutorial_runs(name, tmp_path, monkeypatch):
    """Every python block of the tutorial runs, and every Output block matches its code."""
    monkeypatch.chdir(tmp_path)
    before = threading.active_count()
    ran = run_tutorial(os.path.join(TUTORIAL_DIR, name))
    assert ran >= 3, f"{name}: a tutorial with {ran} runnable blocks is not a tutorial"
    # A tutorial closes what it opens: no thread outlives it.
    assert threading.active_count() <= before, [t.name for t in threading.enumerate()]


def test_every_tutorial_is_listed_in_the_index():
    """A tutorial that is not in tutorials/index.rst is never built into the site."""
    with open(os.path.join(TUTORIAL_DIR, "index.rst"), encoding="utf-8") as f:
        index = f.read()
    for name in tutorial_files():
        assert re.search(rf"^\s+{re.escape(name[:-4])}\s*$", index, re.M), f"{name} is not in tutorials/index.rst"


def test_the_parser_reads_options_captions_and_bodies():
    """The block parser keeps captions, drops options and dedents bodies."""
    text = (".. code-block:: python\n   :emphasize-lines: 1\n\n   x = 1\n   print(x)\n\n"
            "Prose.\n\n.. code-block:: text\n   :caption: Output\n\n   1\n")
    assert parse_blocks(text) == [(1, "python", None, "x = 1\nprint(x)"), (9, "text", "Output", "1")]


if __name__ == "__main__":
    import tempfile
    update = "--update" in sys.argv
    targets = [os.path.abspath(t) for t in sys.argv[1:] if t != "--update"]
    os.chdir(tempfile.mkdtemp(prefix="tutorial-", dir=os.environ.get("TUTORIAL_TMP")))
    for target in targets:
        def show(line, source, output):
            print(f"=== block at line {line}\n{output}", end="" if output.endswith("\n") or not output else "\n")
        run_tutorial(target, show, update)
