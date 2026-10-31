"""Dataset placeholders and explicit boundaries for authored prompt rendering.

Dataset aliases use ``{{ds:alias}}``. :func:`render_template` provides single-pass
substitution: neither inserted dataset values nor inserted native values are
interpreted again. Use it for authored fields without subsequent native formatting.

:func:`render_question_line` is the input-file boundary: it expands aliases written
into one ``input/questions.txt`` line and can append a second alias value to that line.
Input files are read as plain text, so unlike authored prompts they carry no native
placeholders and need no compatibility renderer.

:func:`render_with_native_placeholders` preserves the generator's ordered native
``str.replace`` operations, including their cascading behavior. It temporarily
protects authored dataset placeholders, performs those native operations, and only
then inserts the frozen dataset strings. Neither helper treats imported data or
generated conversation as an authored template. No application modules, GUI,
network clients, or random-number generators are imported here.
"""

import re
from itertools import chain
from typing import Iterable, Iterator, Mapping, Optional, Tuple


class DatasetTemplateError(ValueError):
    """An alias or a dataset placeholder cannot be rendered safely."""


_ALIAS = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_DATASET_PLACEHOLDER = re.compile(r"\{\{ds:([A-Za-z_][A-Za-z0-9_]*)\}\}\Z")
# Whitespace in the reserved namespace is detected so that a mistyped dataset
# placeholder raises an error, instead of silently reaching the model unchanged.
# render_template recognizes native placeholders with exactly one brace on each
# side. The compatibility renderer instead uses the original literal native
# replacements, which can match the {context} substring inside {{context}}.
_TOKEN_START = re.compile(
    r"(?P<dataset>\{\{\s*ds\s*:)|"
    r"(?P<native>(?<!\{)\{[A-Za-z_][A-Za-z0-9_]*\}(?!\}))"
)


DEFAULT_QUESTION_SEPARATOR = "\n\n"


def validate_alias(alias: str) -> str:
    """Return ``alias`` unchanged, or raise :class:`DatasetTemplateError`.

    Aliases are case-sensitive ASCII identifiers: a letter or underscore first,
    followed by letters, digits, or underscores. They do not include ``ds:`` or
    braces. Column names are independent and need not follow these restrictions.
    """
    if not isinstance(alias, str) or _ALIAS.fullmatch(alias) is None:
        raise DatasetTemplateError(
            "Dataset aliases must start with a letter or underscore and contain "
            "only letters, digits, or underscores; enter the alias without "
            "'ds:' or braces."
        )
    return alias


def _tokens(template: str) -> Iterator[Tuple[int, int, str, str]]:
    """Yield positions, kind, and name from original template text only."""
    if not isinstance(template, str):
        raise TypeError("A prompt template must be a string.")
    position = 0
    while True:
        match = _TOKEN_START.search(template, position)
        if match is None:
            return
        start = match.start()
        if match.group("dataset") is not None:
            closing = template.find("}}", match.end())
            if closing == -1:
                raise DatasetTemplateError(
                    "Unclosed dataset placeholder at character {}. "
                    "Use {{{{ds:alias}}}}.".format(start)
                )
            end = closing + 2
            placeholder = _DATASET_PLACEHOLDER.fullmatch(template[start:end])
            if placeholder is None:
                raise DatasetTemplateError(
                    "Malformed dataset placeholder at character {}. "
                    "Use {{{{ds:alias}}}} without spaces, with a valid alias.".format(start)
                )
            yield start, end, "dataset", placeholder.group(1)
        else:
            end = match.end()
            yield start, end, "native", template[start + 1:end - 1]
        position = end


def dataset_placeholders(template: str) -> Tuple[str, ...]:
    """List unique dataset aliases in template order without reading any data.

    Malformed reserved placeholders raise :class:`DatasetTemplateError`, just as
    they do during rendering. Other brace syntax and native placeholders are
    ignored. This can be used to check mappings before scheduling generation.
    """
    aliases = []
    seen = set()
    for _, _, kind, name in _tokens(template):
        if kind == "dataset" and name not in seen:
            aliases.append(name)
            seen.add(name)
    return tuple(aliases)


def render_template(
    template: str,
    dataset_values: Mapping[str, str],
    native_values: Optional[Mapping[str, str]] = None,
) -> str:
    """Substitute known placeholders once, without interpreting inserted text.

    ``dataset_values`` maps aliases to already frozen strings. Every dataset
    placeholder must have a mapping; missing mappings and malformed reserved
    syntax raise :class:`DatasetTemplateError`. Native placeholders are replaced
    only when present in ``native_values``. Unknown native placeholders and
    unrelated braces (including ``{{context}}`` and JSON) remain unchanged.

    Referenced values must be strings; empty strings are valid. This intentionally
    does not guess how to serialize lists, dictionaries, numbers, or nulls. The
    binding layer decides their text representation once per generation job.

    Example::

        render_template(
            "Name: {{ds:name}}; passage: {context}",
            {"name": "Ari {context}"},
            {"context": "Literal {{ds:name}} text"},
        )
        # 'Name: Ari {context}; passage: Literal {{ds:name}} text'
    """
    if native_values is None:
        native_values = {}
    pieces = []
    previous = 0
    for start, end, kind, name in _tokens(template):
        pieces.append(template[previous:start])
        if kind == "dataset":
            if name not in dataset_values:
                raise DatasetTemplateError(
                    "No dataset column is mapped to alias {!r} "
                    "({{{{ds:{}}}}}).".format(name, name)
                )
            value = dataset_values[name]
        elif name in native_values:
            value = native_values[name]
        else:
            value = template[start:end]
        if not isinstance(value, str):
            raise DatasetTemplateError(
                "The value for {} placeholder {!r} must already be a string.".format(kind, name)
            )
        pieces.append(value)
        previous = end
    pieces.append(template[previous:])
    return "".join(pieces)


def render_question_line(
    line: str,
    dataset_values: Optional[Mapping[str, str]],
    append_alias: Optional[str] = None,
    separator: str = DEFAULT_QUESTION_SEPARATOR,
) -> str:
    """Expand one ``input/questions.txt`` line for the task that will use it.

    ``line`` is the sanitized question text read from the file, so it may contain
    ``{{ds:alias}}`` placeholders but never native single-brace ones. Substitution is
    single-pass: neither an expanded value nor an appended value is scanned again, so a
    dataset cell that happens to contain ``{{ds:other}}`` stays literal.

    ``dataset_values`` is the frozen mapping for one task, or ``None`` when the run has no
    dataset attachments at all. ``None`` returns ``line`` unchanged and never appends, which
    keeps a dataset-free run byte-identical to the pre-dataset engine.

    ``append_alias`` names one alias whose value is appended to every line, as a separate
    block after ``separator``. This is the "feed it a variable" form: the question text stays
    a plain authored line while the per-task value rides along with it. An empty alias value
    appends nothing (no dangling separator), and an empty line is never prefixed with one.

    Raises :class:`DatasetTemplateError` for a malformed placeholder, an unmapped alias, a
    non-string value, or an invalid ``append_alias``.
    """
    if not isinstance(line, str):
        raise TypeError("A question line must be a string.")
    rendered = line
    if dataset_values is not None:
        rendered = render_template(line, dataset_values)
    if append_alias is None or append_alias == "":
        return rendered
    validate_alias(append_alias)
    if dataset_values is None:
        raise DatasetTemplateError(
            "Cannot append dataset alias {!r} to a question line: this run has no dataset "
            "attachments.".format(append_alias)
        )
    if append_alias not in dataset_values:
        raise DatasetTemplateError(
            "No dataset column is mapped to alias {!r} ({{{{ds:{}}}}}), so it cannot be "
            "appended to a question line.".format(append_alias, append_alias)
        )
    if not isinstance(separator, str):
        raise DatasetTemplateError("The question append separator must be a string.")
    appended = dataset_values[append_alias]
    if not isinstance(appended, str):
        raise DatasetTemplateError(
            "The value for appended dataset alias {!r} must already be a string."
            .format(append_alias)
        )
    if not appended:
        return rendered
    if not rendered:
        return appended
    return rendered + separator + appended


def render_with_native_placeholders(
    template: str,
    dataset_values: Mapping[str, str],
    native_replacements: Iterable[Tuple[str, str]],
) -> str:
    """Resolve authored dataset aliases around the original native operations.

    ``native_replacements`` is an ordered iterable of ``(literal_token, value)``
    pairs. Tokens must be nonempty strings and values must already be strings.
    Each pair is applied with ``str.replace`` in exactly that order. Native text
    inserted by an earlier operation can therefore be changed by a later one,
    just as in the original generator; native tokens inside double braces also
    retain their original behavior.

    Only dataset placeholders present in the original authored ``template`` are
    resolved. They are protected until every native operation is complete, so a
    dataset value containing ``{context}`` or ``{{ds:another_alias}}`` stays
    literal. Dataset-looking text introduced through a native value stays literal
    too. Malformed or unmapped authored dataset placeholders fail before native
    formatting, even if a native operation would otherwise remove them.

    Do not run further native substitutions over the returned string. Call this
    helper at the existing native-formatting boundary, using the same values and
    order that the generator would have used without dataset placeholders.
    """
    replacements = tuple(native_replacements)
    for token, value in replacements:
        if not isinstance(token, str) or not token:
            raise DatasetTemplateError("Native replacement tokens must be nonempty strings.")
        if not isinstance(value, str):
            raise DatasetTemplateError("Native replacement values must already be strings.")

    placeholders = []
    selected_values = {}
    for start, end, kind, name in _tokens(template):
        if kind != "dataset":
            continue
        if name not in selected_values:
            if name not in dataset_values:
                raise DatasetTemplateError(
                    "No dataset column is mapped to alias {!r} "
                    "({{{{ds:{}}}}}).".format(name, name)
                )
            value = dataset_values[name]
            if not isinstance(value, str):
                raise DatasetTemplateError(
                    "The value for dataset placeholder {!r} must already be a string.".format(name)
                )
            selected_values[name] = value
        placeholders.append((start, end, name))

    if not placeholders:
        for token, value in replacements:
            template = template.replace(token, value)
        return template

    # Each marker is one character absent from EVERY input. Nonempty native
    # tokens cannot match or alter it, and native replacements cannot create it.
    # Prefer private-use characters; fall back to other Unicode scalar values
    # if the authored/native/dataset text already occupies those ranges.
    used = set(template)
    for token, value in replacements:
        used.update(token)
        used.update(value)
    for value in selected_values.values():
        used.update(value)
    available = (
        chr(codepoint)
        for codepoint in chain(
            range(0xE000, 0xF900),
            range(0xF0000, 0xFFFFE),
            range(0x100000, 0x10FFFE),
            range(0x110000),
        )
        if not 0xD800 <= codepoint <= 0xDFFF
    )
    markers = {}
    translations = {}
    for name, value in selected_values.items():
        marker = next((candidate for candidate in available if candidate not in used), None)
        if marker is None:
            raise DatasetTemplateError("Cannot reserve an unambiguous dataset placeholder marker.")
        used.add(marker)
        markers[name] = marker
        translations[ord(marker)] = value

    pieces = []
    previous = 0
    for start, end, name in placeholders:
        pieces.append(template[previous:start])
        pieces.append(markers[name])
        previous = end
    pieces.append(template[previous:])
    formatted = "".join(pieces)
    for token, value in replacements:
        formatted = formatted.replace(token, value)

    # translate performs one pass: inserted dataset text cannot be interpreted
    # as another dataset reference, a native placeholder, or a marker.
    return formatted.translate(translations)
