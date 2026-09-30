"""
Transit Bilty Image Upload Service
====================================
Uploads a photo of the crossing/transit bilty to the `transit-bilty`
Supabase storage bucket and links it to the right row by gr_no —
`bilty.bilty_image` if the GR is a regular bilty, or
`station_bilty_summary.transit_bilty_image` if it's a station bilty.

One gr_no is never in both tables, so the caller doesn't need to know
which type it is — this looks it up and writes to whichever one matches
(same fallback order used by services/kaat/kaat_update_service.py and
services/pohonch/pohonch_create_service.py).
"""
import time
from typing import Optional
from services.supabase_client import get_supabase

BUCKET = "transit-bilty"

_ALLOWED_CONTENT_TYPES = {
    "image/jpeg": "jpeg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
}
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB


def _extension_for(content_type: str, filename: Optional[str]) -> Optional[str]:
    if content_type in _ALLOWED_CONTENT_TYPES:
        return _ALLOWED_CONTENT_TYPES[content_type]
    if filename and "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext in {"jpeg", "jpg", "png", "webp"}:
            return ext
    return None


def _find_target(sb, gr_no: str) -> Optional[dict]:
    """Returns {"table": ..., "id": ..., "image_column": ...} or None."""
    bilty = (
        sb.table("bilty")
        .select("id")
        .eq("gr_no", gr_no)
        .eq("is_active", True)
        .execute()
    )
    if bilty.data:
        return {"table": "bilty", "id": bilty.data[0]["id"], "image_column": "bilty_image"}

    sbs = (
        sb.table("station_bilty_summary")
        .select("id")
        .eq("gr_no", gr_no)
        .execute()
    )
    if sbs.data:
        return {"table": "station_bilty_summary", "id": sbs.data[0]["id"], "image_column": "transit_bilty_image"}

    return None


def upload_transit_bilty_image(
    gr_no: str,
    file_bytes: bytes,
    content_type: str,
    filename: Optional[str] = None,
) -> dict:
    """
    Uploads file_bytes to the transit-bilty bucket and links the resulting
    public URL to gr_no's row — bilty.bilty_image (regular bilty) or
    station_bilty_summary.transit_bilty_image (station bilty).
    """
    if not gr_no:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}
    if not file_bytes:
        return {"status": "error", "message": "file is empty", "status_code": 400}
    if len(file_bytes) > MAX_FILE_SIZE_BYTES:
        return {"status": "error", "message": "file exceeds 10 MB limit", "status_code": 400}

    ext = _extension_for(content_type, filename)
    if not ext:
        return {"status": "error",
                "message": f"Unsupported file type '{content_type}'. Allowed: jpeg, jpg, png, webp",
                "status_code": 400}

    sb = get_supabase()
    target = _find_target(sb, gr_no)
    if not target:
        return {"status": "error", "message": f"GR '{gr_no}' not found in bilty or station_bilty_summary", "status_code": 404}

    path = f"bilty-images/{gr_no}_{int(time.time() * 1000)}.{ext}"
    sb.storage.from_(BUCKET).upload(path, file_bytes, {"content-type": content_type or f"image/{ext}"})
    url = sb.storage.from_(BUCKET).get_public_url(path).rstrip("?")

    sb.table(target["table"]).update({target["image_column"]: url}).eq("id", target["id"]).execute()

    return {
        "status": "success",
        "message": "Transit bilty image uploaded",
        "gr_no": gr_no,
        "table": target["table"],
        "image_column": target["image_column"],
        "url": url,
        "bucket": BUCKET,
        "path": path,
    }


def get_transit_bilty_image(gr_no: str) -> dict:
    """Fetch the current transit bilty image URL for a gr_no, if any."""
    if not gr_no:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}

    sb = get_supabase()

    bilty = sb.table("bilty").select("bilty_image").eq("gr_no", gr_no).eq("is_active", True).execute()
    if bilty.data:
        return {"status": "success", "gr_no": gr_no, "table": "bilty",
                "url": bilty.data[0].get("bilty_image")}

    sbs = sb.table("station_bilty_summary").select("transit_bilty_image").eq("gr_no", gr_no).execute()
    if sbs.data:
        return {"status": "success", "gr_no": gr_no, "table": "station_bilty_summary",
                "url": sbs.data[0].get("transit_bilty_image")}

    return {"status": "error", "message": f"GR '{gr_no}' not found in bilty or station_bilty_summary", "status_code": 404}
