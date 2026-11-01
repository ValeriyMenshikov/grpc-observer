"""Render docs/demo/demo.gif from the real output of docs/demo/demo.py.

Run: uv run --with pillow python docs/demo/make_gif.py
Font: Menlo on macOS, DejaVu Sans Mono on Linux, or set DEMO_FONT=/path/to/mono.ttf.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
COMMAND = "python docs/demo/demo.py"
FONTS = ["/System/Library/Fonts/Menlo.ttc", "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"]

FONT_SIZE, LINE_HEIGHT, PAD, TITLE_BAR, COLUMNS = 15, 21, 18, 30, 100
BG, TITLE_BG = (30, 32, 40), (44, 46, 56)
TEXT, DIM, PROMPT = (220, 223, 228), (120, 126, 140), (130, 200, 255)
LEVEL = {"INFO": (120, 210, 120), "WARNING": (240, 190, 90), "ERROR": (240, 110, 110)}
TYPE, METHOD = (110, 190, 230), (200, 160, 240)

HEAD = re.compile(r"^(\w+)(\s+)(\S+)(\s+)(\S+)(.*)$")
FIELD = re.compile(r"(\w+=)")


def load_font() -> ImageFont.FreeTypeFont:
    for path in [os.environ.get("DEMO_FONT"), *FONTS]:
        if path and Path(path).exists():
            return ImageFont.truetype(path, FONT_SIZE)
    sys.exit("No monospace font found, set DEMO_FONT=/path/to/mono.ttf")


def colorize(line: str) -> list[tuple[str, tuple[int, int, int]]]:
    """Split one output line into (text, color) segments."""
    segments = []
    rest = line
    if match := HEAD.match(line):
        level, gap1, kind, gap2, method, rest = match.groups()
        segments += [(level, LEVEL.get(level, TEXT)), (gap1, TEXT), (kind, TYPE), (gap2, TEXT), (method, METHOD)]
    for part in FIELD.split(rest):
        segments.append((part, DIM if FIELD.fullmatch(part) else TEXT))
    return segments


def render(font, lines: list[list[tuple[str, tuple[int, int, int]]]], rows: int, cursor: bool) -> Image.Image:
    char_width = font.getlength("m")
    image = Image.new("RGB", (int(2 * PAD + COLUMNS * char_width), TITLE_BAR + 2 * PAD + rows * LINE_HEIGHT), BG)
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, image.width, TITLE_BAR), fill=TITLE_BG)
    for i, color in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        x, y = PAD + i * 20, TITLE_BAR // 2
        draw.ellipse((x - 6, y - 6, x + 6, y + 6), fill=color)
    for row, segments in enumerate(lines):
        x, y = PAD, TITLE_BAR + PAD + row * LINE_HEIGHT
        for text, color in segments:
            draw.text((x, y), text, font=font, fill=color)
            x += font.getlength(text)
        if cursor and row == len(lines) - 1:
            draw.rectangle((x + 1, y + 2, x + char_width, y + LINE_HEIGHT - 3), fill=TEXT)
    return image


def main() -> None:
    root = HERE.parents[1]
    output = subprocess.run(
        [sys.executable, str(HERE / "demo.py")], cwd=root, capture_output=True, text=True, check=True
    ).stdout.splitlines()
    font = load_font()
    rows = len(output) + 2
    prompt = [("$ ", PROMPT)]

    frames, durations = [], []

    def frame(lines, duration, cursor=False):
        frames.append(render(font, lines, rows, cursor))
        durations.append(duration)

    frame([prompt], 600, cursor=True)
    for end in range(2, len(COMMAND) + 2, 2):
        frame([[*prompt, (COMMAND[:end], TEXT)]], 60, cursor=True)
    shown = [[*prompt, (COMMAND, TEXT)]]
    frame(shown, 500)
    for line in output:
        shown.append(colorize(line))
        # A short pause after the last line of each call.
        frame(shown, 450 if line.startswith(" ") and "count=" in line or "details=" in line else 110)
    frame([*shown, prompt], 5000, cursor=True)

    palette = frames[-1].convert("P", palette=Image.Palette.ADAPTIVE, colors=128)
    frames = [f.quantize(palette=palette, dither=Image.Dither.NONE) for f in frames]
    target = HERE / "demo.gif"
    frames[0].save(target, save_all=True, append_images=frames[1:], duration=durations, loop=0, optimize=True)
    print(f"{target.relative_to(root)}: {len(frames)} frames, {target.stat().st_size // 1024} KB")


if __name__ == "__main__":
    main()
