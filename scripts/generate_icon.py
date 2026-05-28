#!/usr/bin/env python3
"""Generate professional Marrow application icons at multiple sizes.

Design: "The Core Graph" — a dark rounded-square badge with a central bright
node (the "marrow") connected to satellite AST nodes via clean graph edges.
Represents Marrow's role as an AST dependency-graph context engine.
"""

import math
import os
from PIL import Image, ImageDraw, ImageFilter

# ── Brand palette ──────────────────────────────────────────────────────────
BG_DARK   = (20, 27, 45)       # #141B2D  deep navy
BG_MID    = (28, 37, 65)       # #1C2541  lighter navy (gradient stop)
GREEN     = (74, 181, 111)     # #4AB56F  brand green
GREEN_LT  = (125, 212, 154)   # #7DD49A  highlight/glow
GREEN_DK  = (45, 138, 78)     # #2D8A4E  shadow/depth
LINE_CLR  = (58, 157, 95, 140) # #3A9D5F semi-transparent lines
NODE_SM   = (74, 181, 111, 200) # satellite nodes (slightly transparent)


def _radial_gradient(size, center, radius, c_inner, c_outer):
    """Return an RGBA image with a radial gradient."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    px = img.load()
    cx, cy = center
    for y in range(size):
        for x in range(size):
            d = math.hypot(x - cx, y - cy)
            t = min(d / radius, 1.0)
            r = int(c_inner[0] * (1 - t) + c_outer[0] * t)
            g = int(c_inner[1] * (1 - t) + c_outer[1] * t)
            b = int(c_inner[2] * (1 - t) + c_outer[2] * t)
            a = int(c_inner[3] * (1 - t) + c_outer[3] * t) if len(c_inner) > 3 else 255
            px[x, y] = (r, g, b, a)
    return img


def _rounded_rect_mask(size, radius):
    """Create an alpha mask for a rounded rectangle."""
    # Draw at 4x for anti-aliasing, then downscale
    scale = 4
    big = Image.new("L", (size * scale, size * scale), 0)
    d = ImageDraw.Draw(big)
    d.rounded_rectangle(
        [0, 0, size * scale - 1, size * scale - 1],
        radius=radius * scale,
        fill=255,
    )
    return big.resize((size, size), Image.LANCZOS)


def generate_icon(size=512):
    """Generate the Marrow icon at the given size."""
    S = size
    img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # ── 1. Background rounded square ────────────────────────────────────
    corner_r = int(S * 0.22)  # ~22% corner radius (iOS-ish)
    
    # Create background with subtle radial gradient
    bg = Image.new("RGBA", (S, S), BG_DARK + (255,))
    bg_grad = _radial_gradient(
        S, (S * 0.45, S * 0.4), S * 0.7,
        BG_MID + (60,), (0, 0, 0, 0)
    )
    bg = Image.alpha_composite(bg, bg_grad)
    
    # Apply rounded rectangle mask
    mask = _rounded_rect_mask(S, corner_r)
    img.paste(bg, (0, 0), mask)

    # We need a new draw context since we pasted
    draw = ImageDraw.Draw(img)

    # ── 2. Define node positions ────────────────────────────────────────
    cx, cy = S * 0.48, S * 0.47  # center node, slightly up-left for dynamism
    center_r = S * 0.11  # central node radius

    # Satellite nodes: (angle_deg, distance_ratio, size_ratio)
    satellites = [
        (30,   0.32, 0.045),   # top-right
        (95,   0.28, 0.035),   # right
        (155,  0.35, 0.04),    # bottom-right
        (210,  0.30, 0.05),    # bottom-left
        (280,  0.33, 0.038),   # left
        (340,  0.25, 0.03),    # top
    ]
    
    # Second tier - smaller, further out
    satellites2 = [
        (15,   0.42, 0.025),
        (65,   0.40, 0.028),
        (130,  0.43, 0.022),
        (185,  0.42, 0.025),
        (245,  0.38, 0.03),
        (310,  0.41, 0.022),
    ]

    def sat_pos(angle_deg, dist_ratio):
        rad = math.radians(angle_deg)
        x = cx + math.cos(rad) * S * dist_ratio
        y = cy - math.sin(rad) * S * dist_ratio
        return (x, y)

    # ── 3. Draw connection lines (behind nodes) ────────────────────────
    lines_layer = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    ld = ImageDraw.Draw(lines_layer)
    line_w = max(1, int(S * 0.006))
    thin_w = max(1, int(S * 0.004))
    
    # Primary connections
    for angle, dist, _ in satellites:
        sx, sy = sat_pos(angle, dist)
        ld.line([(cx, cy), (sx, sy)], fill=LINE_CLR, width=line_w)
    
    # Secondary connections (thinner, more transparent)
    sec_line = (58, 157, 95, 80)
    for angle, dist, _ in satellites2:
        sx, sy = sat_pos(angle, dist)
        # Connect to nearest primary satellite
        ld.line([(cx, cy), (sx, sy)], fill=sec_line, width=thin_w)
    
    # Some cross-connections between satellites for graph feel
    cross_pairs = [(0, 1), (1, 2), (3, 4), (4, 5)]
    cross_line = (58, 157, 95, 50)
    for i, j in cross_pairs:
        p1 = sat_pos(satellites[i][0], satellites[i][1])
        p2 = sat_pos(satellites[j][0], satellites[j][1])
        ld.line([p1, p2], fill=cross_line, width=thin_w)

    # Blur lines slightly for glow effect
    lines_layer = lines_layer.filter(ImageFilter.GaussianBlur(radius=S * 0.003))
    img = Image.alpha_composite(img, lines_layer)

    # ── 4. Draw satellite nodes ─────────────────────────────────────────
    nodes_layer = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    nd = ImageDraw.Draw(nodes_layer)
    
    for angle, dist, sr in satellites:
        sx, sy = sat_pos(angle, dist)
        r = S * sr
        nd.ellipse(
            [sx - r, sy - r, sx + r, sy + r],
            fill=NODE_SM
        )
    
    for angle, dist, sr in satellites2:
        sx, sy = sat_pos(angle, dist)
        r = S * sr
        nd.ellipse(
            [sx - r, sy - r, sx + r, sy + r],
            fill=(74, 181, 111, 120)
        )
    
    img = Image.alpha_composite(img, nodes_layer)

    # ── 5. Central node with glow ───────────────────────────────────────
    # Glow layer
    glow = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    glow_r = center_r * 1.8
    gd.ellipse(
        [cx - glow_r, cy - glow_r, cx + glow_r, cy + glow_r],
        fill=(74, 181, 111, 35)
    )
    glow = glow.filter(ImageFilter.GaussianBlur(radius=S * 0.04))
    img = Image.alpha_composite(img, glow)

    # Central node body
    center_layer = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    cd = ImageDraw.Draw(center_layer)
    
    # Outer ring (darker green)
    ring_r = center_r * 1.05
    cd.ellipse(
        [cx - ring_r, cy - ring_r, cx + ring_r, cy + ring_r],
        fill=GREEN_DK + (255,)
    )
    
    # Main circle
    cd.ellipse(
        [cx - center_r, cy - center_r, cx + center_r, cy + center_r],
        fill=GREEN + (255,)
    )
    
    # Inner highlight (lighter gradient feel)
    hi_r = center_r * 0.6
    hi_x, hi_y = cx - center_r * 0.15, cy - center_r * 0.15
    cd.ellipse(
        [hi_x - hi_r, hi_y - hi_r, hi_x + hi_r, hi_y + hi_r],
        fill=GREEN_LT + (90,)
    )
    
    img = Image.alpha_composite(img, center_layer)

    # ── 6. Subtle border on the rounded square ─────────────────────────
    border_layer = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    bd = ImageDraw.Draw(border_layer)
    bw = max(1, int(S * 0.004))
    bd.rounded_rectangle(
        [bw // 2, bw // 2, S - 1 - bw // 2, S - 1 - bw // 2],
        radius=corner_r,
        outline=(74, 181, 111, 40),
        width=bw,
    )
    # Apply same mask
    border_masked = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    border_masked.paste(border_layer, (0, 0), mask)
    img = Image.alpha_composite(img, border_masked)

    return img


def generate_tray_icon(size=32):
    """Generate a simplified tray icon optimized for small sizes."""
    S = size
    img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    cx, cy = S * 0.5, S * 0.5
    
    # Background circle (simpler than rounded rect at this size)
    bg_r = S * 0.45
    draw.ellipse(
        [cx - bg_r, cy - bg_r, cx + bg_r, cy + bg_r],
        fill=BG_DARK + (255,)
    )
    
    # 3-4 connection lines
    center_r = S * 0.12
    line_w = max(1, int(S * 0.06))
    sats = [
        (45,  0.30, 0.06),
        (160, 0.28, 0.055),
        (270, 0.30, 0.06),
    ]
    
    for angle, dist, sr in sats:
        rad = math.radians(angle)
        sx = cx + math.cos(rad) * S * dist
        sy = cy - math.sin(rad) * S * dist
        draw.line([(cx, cy), (sx, sy)], fill=LINE_CLR, width=line_w)
        r = S * sr
        draw.ellipse([sx - r, sy - r, sx + r, sy + r], fill=GREEN + (220,))
    
    # Central node
    draw.ellipse(
        [cx - center_r, cy - center_r, cx + center_r, cy + center_r],
        fill=GREEN + (255,)
    )
    
    return img


def main():
    assets_dir = os.path.join(os.path.dirname(__file__), "..", "assets")
    os.makedirs(assets_dir, exist_ok=True)

    # Generate master icon at 512
    master = generate_icon(512)
    
    # Save at various sizes
    sizes = {
        "icon_512.png": 512,
        "icon_256.png": 256,
        "icon_128.png": 128,
        "icon_64.png": 64,
    }
    
    master.save(os.path.join(assets_dir, "icon_512.png"))
    for name, sz in sizes.items():
        if sz == 512:
            continue
        resized = master.resize((sz, sz), Image.LANCZOS)
        resized.save(os.path.join(assets_dir, name))

    # Generate optimized tray icons
    tray_32 = generate_tray_icon(32)
    tray_32.save(os.path.join(assets_dir, "tray_32.png"))
    
    tray_16 = generate_tray_icon(16)
    tray_16.save(os.path.join(assets_dir, "tray_16.png"))

    print(f"Icons generated in {os.path.abspath(assets_dir)}/")
    for f in sorted(os.listdir(assets_dir)):
        if f.endswith(".png"):
            path = os.path.join(assets_dir, f)
            im = Image.open(path)
            print(f"  {f}: {im.size[0]}x{im.size[1]}")


if __name__ == "__main__":
    main()
