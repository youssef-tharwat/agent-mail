"""Render four readable CLI workflow scenes. Requires Pillow and macOS fonts."""
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
SCALE = 2
SIZE = (1000, 590)
BG = '#101419'
TEXT = '#eef1f4'
MUTED = '#a3adb8'
ACCENT = '#85ddb7'
SCENES = (
    ('Assign a task', 'coordinator',
     ['agent-mail task create api "Review API" --owner worker'],
     'api  /  worker  /  open', 'Next action: Review API'),
    ('Recover after a reset', 'worker',
     ['agent-mail context'],
     'Same task. Same next action.', 'The assignment is stored in local SQLite.'),
    ('Send the result', 'worker',
     ['agent-mail mail send coordinator "Reviewed abc123" \\',
      '  --task api --key api-result-v1'],
     'Result saved. Waiting for a decision.', 'The message stays pending until explicitly resolved.'),
    ('Accept and resolve', 'coordinator',
     ['agent-mail task update api --version 1 --reason "Reviewed" \\',
      '  --state accepted --accepted-revision abc123 \\',
      '  --resolve 1'],
     'api  /  accepted  /  abc123', 'Task closed and linked message resolved together.'),
)


def render(index: int) -> Image.Image:
    """Draw one scene with a command and a clearly labeled state summary."""
    canvas = Image.new('RGB', (SIZE[0] * SCALE, SIZE[1] * SCALE), BG)
    draw = ImageDraw.Draw(canvas)

    def text(x: int, y: int, value: str, size: int, color: str = TEXT,
             *, mono: bool = False) -> None:
        font_path = '/System/Library/Fonts/Menlo.ttc' if mono else '/System/Library/Fonts/SFNS.ttf'
        font = ImageFont.truetype(font_path, size * SCALE)
        draw.text((x * SCALE, y * SCALE), value, font=font, fill=color)

    def line(x1: int, y: int, x2: int, color: str, width: int = 1) -> None:
        draw.line((x1 * SCALE, y * SCALE, x2 * SCALE, y * SCALE), fill=color, width=width * SCALE)

    title, role, commands, result, detail = SCENES[index]
    text(40, 30, 'Agent Mail', 30)
    text(40, 74, 'Durable tasks and messages for coding agents.', 21, MUTED)
    text(825, 40, 'LOCAL SQLITE', 13, MUTED, mono=True)
    line(40, 119, 960, '#303741')
    text(40, 147, f'0{index + 1}', 27, ACCENT, mono=True)
    text(99, 143, title, 32)
    text(40, 213, role, 16, ACCENT, mono=True)
    for offset, command in enumerate(commands):
        text(40, 249 + offset * 30, command, 20, mono=True)
    line(40, 366, 960, '#303741')
    text(40, 393, 'STATE SUMMARY', 13, MUTED, mono=True)
    text(40, 422, result, 26)
    text(40, 461, detail, 20, MUTED)
    labels = ('Assign', 'Recover', 'Send', 'Resolve')
    for step, label in enumerate(labels):
        x = 40 + step * 236
        color = ACCENT if step == index else '#4c5662'
        line(x, 531, x + 210, color, 3)
        text(x, 548, label, 16, color)
    return canvas.resize(SIZE, Image.Resampling.LANCZOS)


def main() -> None:
    """Write the README GIF."""
    frames = [render(index) for index in range(len(SCENES))]
    frames[0].save(ROOT / 'assets/agent-mail-demo.gif', save_all=True,
                   append_images=frames[1:], duration=[4500, 4500, 5000, 6000],
                   loop=0, optimize=True, disposal=2)



if __name__ == '__main__':
    main()
