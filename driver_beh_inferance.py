
# ══════════════════════════════════════════════════════════════════════════════
#  STANDALONE IMAGE DETECTION — full cycle (no prior cells required)
# ══════════════════════════════════════════════════════════════════════════════
import os, warnings
from pathlib import Path

import cv2
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from PIL import Image
import onnxruntime as ort

warnings.filterwarnings("ignore")

# ── Configuration ──────────────────────────────────────────────────────────────
CLASS_NAMES = [
    "DriverBehaviour",  # class 0
    "cigarette",        # class 1
    "foodItem",         # class 2
    "phone",            # class 3
    "seatbelt",         # class 4
    "wheel",            # class 5
]

PALETTE = [
    "#FF4C4C",  # DriverBehaviour  — red
    "#FF9900",  # cigarette        — orange
    "#33CC33",  # foodItem         — green
    "#3399FF",  # phone            — blue
    "#CC66FF",  # seatbelt         — purple
    "#FFD700",  # wheel            — gold
]

CONF_THRESHOLD = 0.35
INPUT_SIZE     = 576

IMG_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
IMG_STD  = np.array([0.229, 0.224, 0.225], dtype=np.float32)

# ── Locate the ONNX model ──────────────────────────────────────────────────────
IN_COLAB  = os.path.isdir("/content")
BASE_DIR  = Path("/content") if IN_COLAB else Path(".")

ONNX_MODEL_PATH = str(BASE_DIR / "rf_detr_driver_behaviour_optimized.onnx")
assert os.path.exists(ONNX_MODEL_PATH), (
    f"ONNX model not found at: {ONNX_MODEL_PATH}\n"
    "Run the export cells first, or set ONNX_MODEL_PATH to the correct path."
)

# ── Image to run detection on ──────────────────────────────────────────────────
# Set this to the path of your image, or leave as None to use the first frame
# of test.mp4 as a demo.
IMAGE_PATH = None   # e.g. IMAGE_PATH = "my_photo.jpg"

# ── Build ORT session ──────────────────────────────────────────────────────────
so = ort.SessionOptions()
so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
so.intra_op_num_threads = os.cpu_count() or 4
so.inter_op_num_threads = os.cpu_count() or 4

available = ort.get_available_providers()
if "CUDAExecutionProvider" in available:
    providers = [("CUDAExecutionProvider", {"device_id": 0}), "CPUExecutionProvider"]
    device_label = "CUDA"
else:
    providers = ["CPUExecutionProvider"]
    device_label = "CPU"

session      = ort.InferenceSession(ONNX_MODEL_PATH, sess_options=so, providers=providers)
input_name   = session.get_inputs()[0].name
output_names = [o.name for o in session.get_outputs()]
print(f"Model      : {ONNX_MODEL_PATH}")
print(f"Provider   : {device_label}  →  {session.get_providers()}")
print(f"Input      : {input_name}  {session.get_inputs()[0].shape}")
print(f"Outputs    : {output_names}")


# ── Helper: preprocess ─────────────────────────────────────────────────────────
def _preprocess(pil_img: Image.Image) -> np.ndarray:
    img = pil_img.convert("RGB").resize((INPUT_SIZE, INPUT_SIZE), Image.BILINEAR)
    arr = np.array(img, dtype=np.float32) / 255.0
    arr = (arr - IMG_MEAN) / IMG_STD
    return np.ascontiguousarray(arr.transpose(2, 0, 1)[np.newaxis])   # NCHW


# ── Helper: postprocess ────────────────────────────────────────────────────────
def _postprocess(raw_outputs, orig_w, orig_h, threshold=CONF_THRESHOLD):
    out_dict = dict(zip(output_names, raw_outputs))

    logits = boxes = None
    for arr in out_dict.values():
        if arr.ndim == 3:
            if arr.shape[-1] == 4:
                boxes  = arr[0]
            else:
                logits = arr[0]

    if logits is None or boxes is None:
        if len(raw_outputs) >= 2:
            logits = raw_outputs[0][0]
            boxes  = raw_outputs[1][0]
        else:
            return [], [], []

    scores      = 1.0 / (1.0 + np.exp(-logits.astype(np.float32)))
    class_ids   = scores.argmax(axis=-1)
    confidences = scores[np.arange(len(class_ids)), class_ids]

    keep = confidences >= threshold
    if not keep.any():
        return [], [], []

    class_ids   = class_ids[keep]
    confidences = confidences[keep]
    bx          = boxes[keep]

    cx, cy, w, h = bx[:, 0], bx[:, 1], bx[:, 2], bx[:, 3]
    x1 = np.clip((cx - w / 2) * orig_w, 0, orig_w)
    y1 = np.clip((cy - h / 2) * orig_h, 0, orig_h)
    x2 = np.clip((cx + w / 2) * orig_w, 0, orig_w)
    y2 = np.clip((cy + h / 2) * orig_h, 0, orig_h)

    xyxy = np.stack([x1, y1, x2, y2], axis=1)
    return xyxy, class_ids, confidences


# ── Load image ─────────────────────────────────────────────────────────────────
if IMAGE_PATH and os.path.exists(IMAGE_PATH):
    pil_img = Image.open(IMAGE_PATH).convert("RGB")
    print(f"\nImage      : {IMAGE_PATH}  ({pil_img.width}×{pil_img.height})")
else:
    VIDEO_PATH = str(BASE_DIR / "test.mp4")
    assert os.path.exists(VIDEO_PATH), (
        "No IMAGE_PATH set and test.mp4 not found. "
        "Set IMAGE_PATH to a valid image file."
    )
    cap = cv2.VideoCapture(VIDEO_PATH)
    ret, bgr = cap.read()
    cap.release()
    assert ret, "Could not read first frame from test.mp4"
    pil_img = Image.fromarray(cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB))
    print(f"\nImage      : first frame of test.mp4  ({pil_img.width}×{pil_img.height})")

orig_w, orig_h = pil_img.size


# ── Run inference ──────────────────────────────────────────────────────────────
inp_arr    = _preprocess(pil_img)
raw        = session.run(output_names, {input_name: inp_arr})
xyxy, class_ids, confidences = _postprocess(raw, orig_w, orig_h)

print(f"Detections : {len(class_ids)}")
for i, (box, cid, conf) in enumerate(zip(xyxy, class_ids, confidences)):
    print(f"  [{i+1}] {CLASS_NAMES[int(cid)]:<18s}  conf={conf:.3f}  "
          f"box=[{box[0]:.0f},{box[1]:.0f},{box[2]:.0f},{box[3]:.0f}]")


# ── Visualise ──────────────────────────────────────────────────────────────────
def _hex_to_rgb01(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) / 255.0 for i in (0, 2, 4))

fig, ax = plt.subplots(1, 1, figsize=(14, 8))
ax.imshow(pil_img)
ax.axis("off")

for box, cid, conf in zip(xyxy, class_ids, confidences):
    cid   = int(cid)
    color = _hex_to_rgb01(PALETTE[cid % len(PALETTE)])
    x1, y1, x2, y2 = box
    w_box, h_box   = x2 - x1, y2 - y1

    rect = mpatches.FancyBboxPatch(
        (x1, y1), w_box, h_box,
        boxstyle="square,pad=0",
        linewidth=2.5, edgecolor=color, facecolor="none"
    )
    ax.add_patch(rect)

    label = f"{CLASS_NAMES[cid]}  {conf:.2f}"
    ax.text(
        x1, max(y1 - 5, 0), label,
        fontsize=10, fontweight="bold", color="white",
        bbox=dict(facecolor=color, edgecolor="none", boxstyle="round,pad=0.25", alpha=0.85),
    )

# Legend
legend_patches = [
    mpatches.Patch(color=_hex_to_rgb01(PALETTE[i]), label=CLASS_NAMES[i])
    for i in range(len(CLASS_NAMES))
]
ax.legend(handles=legend_patches, loc="upper right", fontsize=9,
          framealpha=0.8, title="Classes", title_fontsize=10)

source_label = Path(IMAGE_PATH).name if IMAGE_PATH else "test.mp4 — frame 0"
ax.set_title(
    f"RF-DETR ONNX Detection  ·  {source_label}  ·  "
    f"{len(class_ids)} detection(s)  ·  {device_label}",
    fontsize=12, fontweight="bold", pad=10,
)

plt.tight_layout()

# Save annotated image
out_dir = BASE_DIR / "output_onnx"
out_dir.mkdir(parents=True, exist_ok=True)
out_path = out_dir / "image_detection_result.jpg"
fig.savefig(str(out_path), dpi=150, bbox_inches="tight")
plt.show()
print(f"\nAnnotated image saved → {out_path}")
