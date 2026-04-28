"""
Ana DOI Zenginleştirme Scripti — Production
============================================
Akademik makale DOI'larını OpenAlex (birincil) + Crossref (fallback) üzerinden
zenginleştirir. Checkpointing, resume, retry-with-exponential-backoff,
paralel istek ve başarısız DOI takibi destekler.


Çıktılar (OUTPUT_DIR içinde):
  - enriched.parquet      : zenginleştirilmiş veri (DOI primary key)
  - failed_dois.csv       : bulunamayan / hata alan DOI'ler
  - enrichment_log.txt    : çalıştırma logu
"""

import pandas as pd
import requests
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime

try:
    from tqdm import tqdm
except ImportError:
    raise SystemExit("tqdm yüklü değil. Çalıştır: pip install tqdm")


# =============================================================================
# KONFİGÜRASYON — BURAYI GÜNCELLE
# =============================================================================
OPENALEX_API_KEY = "**********************"        # openalex.org/settings/api
CONTACT_EMAIL    = "******@gmail.com"   # Crossref polite pool

# Input CSV dosyaları
INPUT_FILES = {
    "train": r"C:\Users\Kerem\Desktop\proje\train.csv",
    "test":  r"C:\Users\Kerem\Desktop\proje\test.csv",
}
DOI_COLUMN = "doi"   # CSV'deki DOI sütununun adı

# Output
OUTPUT_DIR   = Path(r"C:\Users\Kerem\Desktop\proje\enriched")
OUTPUT_FILE  = "enriched.parquet"
FAILED_FILE  = "failed_dois.csv"
LOG_FILE     = "enrichment_log.txt"

# Performans / politeness
WORKERS              = 5       # paralel thread sayısı (1 = sequential; 5 = güvenli, hızlı)
INTER_REQUEST_DELAY  = 0.12    # her thread'in kendi arasında bekleme (sn)
CHECKPOINT_EVERY     = 200     # her N başarılı sonuç → diske yaz

# Retry
MAX_RETRIES       = 5
RETRY_BASE_DELAY  = 2          # saniye (exponential: 2, 4, 8, 16, 32)
REQUEST_TIMEOUT   = 30

# =============================================================================


# Thread-safe buffer'lar
_results_buffer = []
_failed_buffer  = []
_buffer_lock    = Lock()
_log_lock       = Lock()


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def log(msg: str, path: Path = None):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if path:
        with _log_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")


# ---------------------------------------------------------------------------
# DOI NORMALİZASYON
# ---------------------------------------------------------------------------
def normalize_doi(doi) -> str | None:
    if not isinstance(doi, str):
        return None
    doi = doi.strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi.org/", "doi:"):
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
            break
    return doi if doi else None


# ---------------------------------------------------------------------------
# OPENALEX
# ---------------------------------------------------------------------------
def fetch_from_openalex(doi: str, api_key: str) -> dict:
    url = f"https://api.openalex.org/works/https://doi.org/{doi}"
    params = {"api_key": api_key}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return {"status": "success", "data": r.json()}
        if r.status_code == 404:
            return {"status": "not_found", "data": None}
        if r.status_code == 429:
            return {"status": "rate_limit",
                    "retry_after": r.headers.get("retry-after"), "data": None}
        if r.status_code == 403:
            return {"status": "forbidden_check_api_key", "data": None}
        if 500 <= r.status_code < 600:
            return {"status": f"server_error_{r.status_code}", "data": None}
        return {"status": f"http_{r.status_code}", "data": None}
    except requests.exceptions.Timeout:
        return {"status": "timeout", "data": None}
    except requests.exceptions.RequestException as e:
        return {"status": "network_error", "error": str(e), "data": None}


def extract_openalex_fields(data: dict) -> dict:
    if not data:
        return None

    authors = []
    for authorship in (data.get("authorships") or []):
        a = authorship.get("author") or {}
        institutions = [
            {
                "name":    inst.get("display_name"),
                "ror":     inst.get("ror"),
                "country": inst.get("country_code"),
                "type":    inst.get("type"),
            }
            for inst in (authorship.get("institutions") or [])
        ]
        authors.append({
            "name":                   a.get("display_name"),
            "openalex_id":            a.get("id"),
            "orcid":                  a.get("orcid"),
            "position":               authorship.get("author_position"),
            "is_corresponding":       authorship.get("is_corresponding"),
            "raw_affiliation_strings": authorship.get("raw_affiliation_strings") or [],
            "institutions":           institutions,
        })

    primary = data.get("primary_location") or {}
    source  = primary.get("source") or {}
    topics  = [
        {"name": t.get("display_name"), "score": t.get("score")}
        for t in (data.get("topics") or [])[:5]
    ]

    return {
        "doi":                   normalize_doi(data.get("doi")),
        "openalex_id":           data.get("id"),
        "title":                 data.get("title"),
        "publication_year":      data.get("publication_year"),
        "publication_date":      data.get("publication_date"),
        "type":                  data.get("type"),
        "language":              data.get("language"),
        "cited_by_count":        data.get("cited_by_count"),
        "counts_by_year":        data.get("counts_by_year"),
        "authors_count":         len(authors),
        "authors":               authors,
        "journal_name":          source.get("display_name"),
        "journal_issn_l":        source.get("issn_l"),
        "publisher":             source.get("host_organization_name"),
        "is_oa":                 (data.get("open_access") or {}).get("is_oa"),
        "oa_status":             (data.get("open_access") or {}).get("oa_status"),
        "topics":                topics,
        "referenced_works_count": len(data.get("referenced_works") or []),
        "source":                "openalex",
    }


# ---------------------------------------------------------------------------
# CROSSREF (fallback)
# ---------------------------------------------------------------------------
def fetch_from_crossref(doi: str, email: str) -> dict:
    url = f"https://api.crossref.org/works/{doi}"
    headers = {"User-Agent": f"AcademicQualityResearch/1.0 (mailto:{email})"}
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return {"status": "success", "data": r.json().get("message")}
        if r.status_code == 404:
            return {"status": "not_found", "data": None}
        if 500 <= r.status_code < 600:
            return {"status": f"server_error_{r.status_code}", "data": None}
        return {"status": f"http_{r.status_code}", "data": None}
    except requests.exceptions.Timeout:
        return {"status": "timeout", "data": None}
    except requests.exceptions.RequestException as e:
        return {"status": "network_error", "error": str(e), "data": None}


def extract_crossref_fields(data: dict) -> dict:
    if not data:
        return None

    authors = []
    for a in (data.get("author") or []):
        name = f"{a.get('given', '')} {a.get('family', '')}".strip()
        authors.append({
            "name":                    name,
            "orcid":                   a.get("ORCID"),
            "position":                None,
            "is_corresponding":        None,
            "raw_affiliation_strings": [aff.get("name") for aff in (a.get("affiliation") or [])],
            "institutions":            [],
        })

    pub_parts = ((data.get("published") or {}).get("date-parts") or [[None]])
    pub_year  = pub_parts[0][0] if pub_parts and pub_parts[0] else None

    return {
        "doi":                   normalize_doi(data.get("DOI")),
        "openalex_id":           None,
        "title":                 (data.get("title") or [None])[0],
        "publication_year":      pub_year,
        "publication_date":      None,
        "type":                  data.get("type"),
        "language":              data.get("language"),
        "cited_by_count":        None,                     # Crossref'te yok
        "counts_by_year":        None,
        "authors_count":         len(authors),
        "authors":               authors,
        "journal_name":          (data.get("container-title") or [None])[0],
        "journal_issn_l":        (data.get("ISSN") or [None])[0],
        "publisher":             data.get("publisher"),
        "is_oa":                 None,
        "oa_status":             None,
        "topics":                [],
        "referenced_works_count": data.get("references-count"),
        "source":                "crossref",
    }


# ---------------------------------------------------------------------------
# RETRY WRAPPER
# ---------------------------------------------------------------------------
RETRIABLE = {"rate_limit", "timeout", "network_error"}

def _is_retriable(status: str) -> bool:
    return (status in RETRIABLE) or status.startswith("server_error_")


def fetch_with_retry(fetch_fn, *args):
    last = None
    for attempt in range(MAX_RETRIES):
        result = fetch_fn(*args)
        last = result
        status = result.get("status", "")

        if status in ("success", "not_found", "forbidden_check_api_key"):
            return result
        if not _is_retriable(status):
            return result

        if attempt < MAX_RETRIES - 1:
            if status == "rate_limit" and result.get("retry_after"):
                wait = int(result["retry_after"])
            else:
                wait = RETRY_BASE_DELAY * (2 ** attempt)
            time.sleep(wait)
    return last


# ---------------------------------------------------------------------------
# TEK DOI İŞLEME
# ---------------------------------------------------------------------------
def process_doi(doi: str) -> dict:
    oa = fetch_with_retry(fetch_from_openalex, doi, OPENALEX_API_KEY)

    if oa["status"] == "success":
        parsed = extract_openalex_fields(oa["data"])
        if parsed and not parsed.get("doi"):
            parsed["doi"] = doi  # DOI eksikse input'la doldur
        return parsed

    if oa["status"] == "not_found":
        cr = fetch_with_retry(fetch_from_crossref, doi, CONTACT_EMAIL)
        if cr["status"] == "success":
            parsed = extract_crossref_fields(cr["data"])
            if parsed and not parsed.get("doi"):
                parsed["doi"] = doi
            return parsed
        return {"doi": doi, "source": None, "status": "not_found_anywhere"}

    if oa["status"] == "forbidden_check_api_key":
        return {"doi": doi, "source": None, "status": "api_key_error"}

    return {"doi": doi, "source": None, "status": oa["status"]}


# ---------------------------------------------------------------------------
# CHECKPOINT I/O
# ---------------------------------------------------------------------------
# Parquet'e yazılırken nested list/dict'leri JSON string'e çeviriyoruz.
# Böylece pyarrow backend sorunu yaşamıyor ve okurken json.loads ile geri açılır.
JSON_COLS = ("authors", "counts_by_year", "topics")


def _to_parquet_row(row: dict) -> dict:
    out = dict(row)
    for col in JSON_COLS:
        if col in out and out[col] is not None:
            out[col] = json.dumps(out[col], ensure_ascii=False)
    return out


def load_existing_progress(output_path: Path):
    if not output_path.exists():
        return set()
    try:
        df = pd.read_parquet(output_path, columns=[DOI_COLUMN])
        return set(df[DOI_COLUMN].dropna().astype(str).str.lower())
    except Exception as e:
        print(f"  Uyarı: {output_path} okunamadı ({e}) — baştan başlanıyor.")
        return set()


def load_existing_failed(failed_path: Path):
    if not failed_path.exists():
        return set()
    try:
        df = pd.read_csv(failed_path)
        return set(df["doi"].dropna().astype(str).str.lower())
    except Exception:
        return set()


def flush_to_disk(output_path: Path, failed_path: Path):
    global _results_buffer, _failed_buffer

    with _buffer_lock:
        res_snapshot = _results_buffer
        fail_snapshot = _failed_buffer
        _results_buffer = []
        _failed_buffer = []

    if res_snapshot:
        rows = [_to_parquet_row(r) for r in res_snapshot]
        new_df = pd.DataFrame(rows)
        if output_path.exists():
            existing = pd.read_parquet(output_path)
            combined = pd.concat([existing, new_df], ignore_index=True)
            # Aynı DOI varsa en sonuncuyu tut
            combined = combined.drop_duplicates(subset=[DOI_COLUMN], keep="last")
        else:
            combined = new_df
        combined.to_parquet(output_path, index=False)

    if fail_snapshot:
        fdf = pd.DataFrame(fail_snapshot)
        if failed_path.exists():
            existing = pd.read_csv(failed_path)
            combined = pd.concat([existing, fdf], ignore_index=True)
            combined = combined.drop_duplicates(subset=["doi"], keep="last")
        else:
            combined = fdf
        combined.to_csv(failed_path, index=False)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    # Sanity check
    if OPENALEX_API_KEY == "YOUR_API_KEY_HERE":
        print(" OPENALEX_API_KEY'i doldurmayı unuttun.")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / OUTPUT_FILE
    failed_path = OUTPUT_DIR / FAILED_FILE
    log_path    = OUTPUT_DIR / LOG_FILE

    log(f"Başlangıç. Workers={WORKERS}, checkpoint_every={CHECKPOINT_EVERY}", log_path)

    # 1) Input CSV'lerden DOI'leri topla
    all_dois = set()
    for label, path in INPUT_FILES.items():
        p = Path(path)
        if not p.exists():
            log(f"  UYARI: {path} bulunamadı, atlanıyor.", log_path)
            continue
        try:
            df = pd.read_csv(p)
        except UnicodeDecodeError:
            df = pd.read_csv(p, encoding="latin-1")

        if DOI_COLUMN not in df.columns:
            log(f"  UYARI: {p.name} içinde '{DOI_COLUMN}' sütunu yok. "
                f"Mevcut sütunlar: {list(df.columns)}", log_path)
            continue

        dois = {normalize_doi(x) for x in df[DOI_COLUMN]}
        dois.discard(None)
        all_dois.update(dois)
        log(f"  {label}: {len(dois)} benzersiz DOI ({p.name})", log_path)

    log(f"Toplam birleşik benzersiz DOI: {len(all_dois)}", log_path)

    # 2) Resume: daha önce işlenmiş ve başarısız olanları çıkar
    processed = load_existing_progress(output_path)
    failed    = load_existing_failed(failed_path)
    log(f"  Daha önce başarıyla işlenmiş : {len(processed)}", log_path)
    log(f"  Daha önce başarısız işaretli : {len(failed)} (retry için istersen failed_dois.csv'yi sil)", log_path)

    remaining = sorted(all_dois - processed - failed)
    log(f"  Bu çalıştırmada işlenecek    : {len(remaining)}", log_path)

    if not remaining:
        log("Yapılacak bir şey yok, çıkılıyor.", log_path)
        return

    # 3) Ana işleme döngüsü (paralel)
    def worker(doi):
        result = process_doi(doi)
        time.sleep(INTER_REQUEST_DELAY)
        return doi, result

    t0 = time.time()
    counter = 0
    ok_oa = ok_cr = fail_cnt = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(worker, doi): doi for doi in remaining}

        with tqdm(total=len(remaining), desc="Enriching", unit="doi") as pbar:
            try:
                for future in as_completed(futures):
                    try:
                        doi, result = future.result()
                    except Exception as e:
                        log(f"Beklenmeyen hata: {e}", log_path)
                        pbar.update(1)
                        continue

                    with _buffer_lock:
                        if result.get("source") == "openalex":
                            _results_buffer.append(result)
                            ok_oa += 1
                        elif result.get("source") == "crossref":
                            _results_buffer.append(result)
                            ok_cr += 1
                        else:
                            _failed_buffer.append({
                                "doi":       doi,
                                "reason":    result.get("status", "unknown"),
                                "timestamp": datetime.now().isoformat(),
                            })
                            fail_cnt += 1
                        counter += 1

                    pbar.update(1)
                    if counter % 50 == 0:
                        pbar.set_postfix({"OA": ok_oa, "CR": ok_cr, "fail": fail_cnt})

                    if counter % CHECKPOINT_EVERY == 0:
                        flush_to_disk(output_path, failed_path)
            except KeyboardInterrupt:
                log("\n  Ctrl+C algılandı — mevcut buffer diske yazılıyor...", log_path)
                flush_to_disk(output_path, failed_path)
                log("  Güvenle durduruldu. Tekrar çalıştırdığında kaldığı yerden devam edecek.", log_path)
                return

    # 4) Son flush
    flush_to_disk(output_path, failed_path)

    # 5) Özet
    dt = time.time() - t0
    log("\n" + "=" * 60, log_path)
    log("TAMAMLANDI", log_path)
    log(f"  OpenAlex başarılı : {ok_oa}", log_path)
    log(f"  Crossref fallback : {ok_cr}", log_path)
    log(f"  Başarısız         : {fail_cnt}", log_path)
    log(f"  Toplam süre       : {dt/60:.1f} dakika  ({dt/max(counter,1):.2f} s/DOI)", log_path)
    log(f"  Ana çıktı         : {output_path}", log_path)
    log(f"  Başarısızlar      : {failed_path}", log_path)


if __name__ == "__main__":
    main()