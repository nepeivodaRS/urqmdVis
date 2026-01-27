import re
from pathlib import Path
from xml.sax.saxutils import escape


def _replace_block(text, proxy_type, replacer):
    pattern = re.compile(
        rf'(<Proxy group="[^"]+" type="{proxy_type}"[^>]*>)(.*?)(</Proxy>)',
        re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        return text
    head, body, tail = match.group(1), match.group(2), match.group(3)
    return text[: match.start()] + head + replacer(body) + tail + text[match.end() :]


def _replace_property(body, prop_name, new_inner):
    pattern = re.compile(rf'(<Property name="{prop_name}"[^>]*>)(.*?)(</Property>)', re.DOTALL)
    return pattern.sub(rf"\1{new_inner}\3", body, count=1)


def _replace_elements(body, prop_name, new_elements):
    pattern = re.compile(rf'(<Property name="{prop_name}"[^>]*>)(.*?)(</Property>)', re.DOTALL)

    def repl(match):
        head, inner, tail = match.group(1), match.group(2), match.group(3)
        inner = re.sub(r"\s*<Element[^>]*/>\s*", "\n", inner)
        return f"{head}\n{new_elements}\n{inner.strip()}\n{tail}"

    return pattern.sub(repl, body, count=1)


def generate_state(template_path, output_path, csv_files, scale, text_block):
    template_path = Path(template_path)
    output_path = Path(output_path)
    csv_files = [str(Path(p).resolve()) for p in csv_files]
    if not csv_files:
        raise ValueError("No CSV files provided for ParaView state.")

    raw = template_path.read_text()

    def update_csv_reader(body):
        file_elems = "".join(
            f'\n        <Element index="{i}" value="{escape(path)}"/>'
            for i, path in enumerate(csv_files)
        )
        body = _replace_elements(body, "FileName", file_elems.strip())
        body = _replace_elements(body, "FileNameInfo", f'<Element index="0" value="{escape(csv_files[0])}"/>')
        timestep_elems = "".join(
            f'\n        <Element index="{i}" value="{i}"/>'
            for i in range(len(csv_files))
        )
        body = _replace_elements(body, "TimestepValues", timestep_elems.strip())
        return body

    def update_time_converter(body):
        body = _replace_elements(body, "Scale", f'<Element index="0" value="{scale}"/>')
        return body

    def update_text_source(body):
        text_value = escape(text_block).replace("\n", "&#xa;")
        body = _replace_elements(body, "Text", f'<Element index="0" value="{text_value}"/>')
        return body

    raw = _replace_block(raw, "CSVReader", update_csv_reader)
    raw = _replace_block(raw, "TimeToTextConvertor", update_time_converter)
    raw = _replace_block(raw, "TextSource", update_text_source)

    # Update pipeline browser item name/logname to match first CSV
    first_name = Path(csv_files[0]).name.replace(".csv", ".csv*")
    raw = re.sub(
        r'(<Item id="31533" name=")[^"]*(" logname=")[^"]*(")',
        rf"\1{first_name}\2{first_name}\3",
        raw,
        count=1,
    )
    # Remove duplicate pipeline items that point to the same CSV reader name.
    item_pattern = re.compile(
        rf'\s*<Item[^>]*name="{re.escape(first_name)}"[^>]*logname="{re.escape(first_name)}"[^>]*/>\s*'
    )
    seen = False

    def _dedupe_item(match):
        nonlocal seen
        if not seen:
            seen = True
            return match.group(0)
        return ""

    raw = item_pattern.sub(_dedupe_item, raw)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(raw)
    return str(output_path)
