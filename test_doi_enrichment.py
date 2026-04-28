"""
DOI Zenginleştirme Test Scripti
================================
Amaç: OpenAlex (birincil) + Crossref (fallback) pipeline'ını küçük örneklemde
test etmek. Ana scripti yazmadan önce her şeyin çalıştığını doğrulamak için.

Kullanım:
  1. Aşağıdaki OPENALEX_API_KEY ve CONTACT_EMAIL değişkenlerini güncelle.
  2. TEST_DOIS listesine kendi veri setinden 5-10 DOI ekle (ya da default'u dene).
  3. python test_doi_enrichment.py
  4. Çıktıya bak: test_results.json dosyasını aç, alanların içeriğini incele.

Gereksinimler:
  pip install requests
"""

import requests
import time
import json
from datetime import datetime

# ============================================================================
# KONFİGÜRASYON - BURAYI GÜNCELLE
# ============================================================================
OPENALEX_API_KEY = "WkkK4Zaj6nLjTUfMo5Yoep"        # openalex.org/settings/api
CONTACT_EMAIL    = "keremkartal491741@gmail.com"   # Crossref polite pool için

# Test edilecek DOI'ler.
# En iyisi kendi veri setinden 5-10 DOI koymak (farklı kategoriler, farklı yıllar).
# Default olarak 3 iyi bilinen makale + 1 kasıtlı yanlış DOI (fallback testi) var.
TEST_DOIS = [
    "10.1038/nature14539",          # LeCun et al. - Deep Learning (Nature 2015) — yüksek atıflı
     "10.1002/ajmg.a.63050",          # LeCun et al. - Deep Learning (Nature 2015) — yüksek atıflı
      "10.1002/anie.202104531",          # LeCun et al. - Deep Learning (Nature 2015) — yüksek atıflı
       "10.1002/ana.26339",          # LeCun et al. - Deep Learning (Nature 2015) — yüksek atıflı
         "10.1002/andp.202300457",          # LeCun et al. - Deep Learning (Nature 2015) — yüksek atıflı        
    "10.1016/S0140-6736(20)30183-5",# Huang et al. - COVID-19 Lancet 2020 — klinik
    "10.1126/science.abb2507",      # Zhou et al. - SARS-CoV-2 Science 2020 — biyoloji
    "10.1234/bu.doi.kesinlikle.yok",# Kasıtlı bozuk — hata/fallback davranışını görmek için
    # "10.xxxx/..."                 # Buraya kendi DOI'lerinden ekle
]


# ============================================================================
# OPENALEX ÇAĞRISI
# ============================================================================
def fetch_from_openalex(doi: str, api_key: str) -> dict:
    """OpenAlex'ten DOI ile tek makale çek. 'Singleton by DOI' = ücretsiz."""
    url = f"https://api.openalex.org/works/https://doi.org/{doi}"
    params = {"api_key": api_key}

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return {"status": "success", "data": r.json(),
                    "credits_remaining": r.headers.get("x-credits-remaining")}
        if r.status_code == 404:
            return {"status": "not_found", "data": None}
        if r.status_code == 429:
            return {"status": "rate_limit", "retry_after": r.headers.get("retry-after"), "data": None}
        if r.status_code == 403:
            return {"status": "forbidden_check_api_key", "data": None}
        return {"status": f"http_{r.status_code}", "data": None, "body": r.text[:200]}
    except requests.exceptions.RequestException as e:
        return {"status": "network_error", "error": str(e), "data": None}


def extract_openalex_fields(data: dict) -> dict:
    """OpenAlex response'undan projemiz için kritik alanları çıkar."""
    if not data:
        return None

    # Yazarlar (isim + OpenAlex ID + ORCID + kurumlar + pozisyon)
    authors = []
    for authorship in data.get("authorships", []):
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
            "name":         a.get("display_name"),
            "openalex_id":  a.get("id"),
            "orcid":        a.get("orcid"),
            "position":     authorship.get("author_position"),
            "institutions": institutions,
        })

    # Dergi/kaynak bilgisi
    primary = data.get("primary_location") or {}
    source  = primary.get("source") or {}

    # Konular (ilk 5)
    topics = [
        {"name": t.get("display_name"), "score": t.get("score")}
        for t in (data.get("topics") or [])[:5]
    ]

    return {
        "openalex_id":        data.get("id"),
        "doi":                data.get("doi"),
        "title":              data.get("title"),
        "publication_year":   data.get("publication_year"),
        "publication_date":   data.get("publication_date"),
        "type":               data.get("type"),
        "cited_by_count":     data.get("cited_by_count"),
        "counts_by_year":     data.get("counts_by_year"),        # ZAMAN SERİSİ ALTINI
        "authors_count":      len(authors),
        "authors":            authors,
        "journal_name":       source.get("display_name"),
        "journal_issn_l":     source.get("issn_l"),
        "publisher":          source.get("host_organization_name"),
        "is_oa":              (data.get("open_access") or {}).get("is_oa"),
        "oa_status":          (data.get("open_access") or {}).get("oa_status"),
        "topics":             topics,
        "referenced_works_count": len(data.get("referenced_works") or []),
        "language":           data.get("language"),
        "source":             "openalex",
    }


# ============================================================================
# CROSSREF FALLBACK
# ============================================================================
def fetch_from_crossref(doi: str, email: str) -> dict:
    """OpenAlex bulamazsa Crossref. Not: Crossref'te atıf SAYISI yoktur."""
    url = f"https://api.crossref.org/works/{doi}"
    headers = {"User-Agent": f"AcademicQualityResearch/1.0 (mailto:{email})"}

    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            return {"status": "success", "data": r.json().get("message")}
        if r.status_code == 404:
            return {"status": "not_found", "data": None}
        return {"status": f"http_{r.status_code}", "data": None}
    except requests.exceptions.RequestException as e:
        return {"status": "network_error", "error": str(e), "data": None}


def extract_crossref_fields(data: dict) -> dict:
    if not data:
        return None

    authors = []
    for a in (data.get("author") or []):
        name = f"{a.get('given', '')} {a.get('family', '')}".strip()
        authors.append({
            "name":         name,
            "orcid":        a.get("ORCID"),
            "affiliations": [aff.get("name") for aff in (a.get("affiliation") or [])],
        })

    pub_parts = ((data.get("published") or {}).get("date-parts") or [[None]])
    pub_year  = pub_parts[0][0] if pub_parts and pub_parts[0] else None

    return {
        "doi":              data.get("DOI"),
        "title":            (data.get("title") or [None])[0],
        "publication_year": pub_year,
        "type":             data.get("type"),
        "cited_by_count":   None,  # Crossref'te yok
        "counts_by_year":   None,
        "authors_count":    len(authors),
        "authors":          authors,
        "journal_name":     (data.get("container-title") or [None])[0],
        "journal_issn":     (data.get("ISSN") or [None])[0],
        "publisher":        data.get("publisher"),
        "referenced_works_count": data.get("references-count"),
        "source":           "crossref",
    }


# ============================================================================
# ANA TEST DÖNGÜSÜ
# ============================================================================
def main():
    if OPENALEX_API_KEY == "YOUR_API_KEY_HERE":
        print(" OPENALEX_API_KEY'i doldurmayı unuttun. Script üstündeki değişkeni güncelle.")
        return

    print("=" * 72)
    print("DOI ZENGİNLEŞTİRME TESTİ")
    print(f"Başlangıç: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 72)

    results = []
    t_global = time.time()

    for i, doi in enumerate(TEST_DOIS, 1):
        print(f"\n[{i}/{len(TEST_DOIS)}] DOI: {doi}")
        print("-" * 62)

        t0 = time.time()
        oa = fetch_from_openalex(doi, OPENALEX_API_KEY)

        if oa["status"] == "success":
            parsed = extract_openalex_fields(oa["data"])
            dt = time.time() - t0
            print(f"  ✓ OpenAlex ({dt:.2f}s)")
            print(f"    Başlık         : {(parsed.get('title') or '')[:80]}")
            print(f"    Yıl            : {parsed.get('publication_year')}")
            print(f"    Yazar sayısı   : {parsed.get('authors_count')}")
            print(f"    Toplam atıf    : {parsed.get('cited_by_count')}")
            by_year = parsed.get("counts_by_year") or []
            print(f"    Yıl-yıl atıf   : {len(by_year)} yıllık veri"
                  + (f"  örn: {by_year[:3]}" if by_year else ""))
            print(f"    Dergi          : {parsed.get('journal_name')}")
            print(f"    Yayıncı        : {parsed.get('publisher')}")
            print(f"    Open Access    : {parsed.get('is_oa')} ({parsed.get('oa_status')})")
            print(f"    Referans sayısı: {parsed.get('referenced_works_count')}")
            if parsed.get("authors"):
                a0 = parsed["authors"][0]
                inst0 = a0["institutions"][0]["name"] if a0.get("institutions") else None
                print(f"    İlk yazar      : {a0.get('name')}  (ORCID: {a0.get('orcid')})")
                print(f"    İlk yazar kurum: {inst0}")
            if oa.get("credits_remaining"):
                print(f"    Kalan kredi    : {oa['credits_remaining']}")
            results.append(parsed)

        elif oa["status"] == "not_found":
            print(f"  OpenAlex'te bulunamadı → Crossref deneniyor...")
            cr = fetch_from_crossref(doi, CONTACT_EMAIL)
            if cr["status"] == "success":
                parsed = extract_crossref_fields(cr["data"])
                dt = time.time() - t0
                print(f"  ✓ Crossref fallback ({dt:.2f}s)")
                print(f"    Başlık         : {(parsed.get('title') or '')[:80]}")
                print(f"    Yıl            : {parsed.get('publication_year')}")
                print(f"    Yazar sayısı   : {parsed.get('authors_count')}")
                print(f"    Dergi          : {parsed.get('journal_name')}")
                print(f"    ⚠  Crossref'te atıf sayısı yok")
                results.append(parsed)
            else:
                print(f"  ✗ İki kaynakta da yok. OA={oa['status']}, CR={cr['status']}")
                results.append({"doi": doi, "source": None, "status": "not_found_anywhere"})

        elif oa["status"] == "forbidden_check_api_key":
            print(f"  ✗ API KEY ÇALIŞMIYOR. openalex.org/settings/api'den kontrol et.")
            results.append({"doi": doi, "source": None, "status": "api_key_error"})
            break  # key bozuksa devam etmenin anlamı yok

        elif oa["status"] == "rate_limit":
            print(f"  ✗ Rate limit (olmaması lazım ama). retry_after={oa.get('retry_after')}")
            time.sleep(int(oa.get("retry_after") or 30))
            results.append({"doi": doi, "source": None, "status": "rate_limited"})

        else:
            print(f"  ✗ OpenAlex hatası: {oa['status']}")
            results.append({"doi": doi, "source": None, "status": oa["status"]})

        time.sleep(0.12)  # saniyede ~8 istek (kibar)

    # ========================================================================
    # ÖZET + DOSYAYA YAZ
    # ========================================================================
    total_time = time.time() - t_global
    print("\n" + "=" * 72)
    print("TEST ÖZETİ")
    print("=" * 72)

    n_oa = sum(1 for r in results if r.get("source") == "openalex")
    n_cr = sum(1 for r in results if r.get("source") == "crossref")
    n_fail = sum(1 for r in results if not r.get("source"))
    n_total = len(results)

    print(f"Toplam DOI           : {n_total}")
    print(f"OpenAlex'ten çekildi : {n_oa}")
    print(f"Crossref fallback    : {n_cr}")
    print(f"Hiç bulunamadı       : {n_fail}")
    print(f"Toplam süre          : {total_time:.2f}s")
    if n_total:
        print(f"DOI başına ortalama  : {total_time/n_total:.2f}s")
        # 23.466 DOI için kaba tahmin
        est_min = (total_time / n_total) * 23466 / 60
        print(f"→ 23.466 DOI için tahmini süre: ~{est_min:.0f} dakika")

    # Ham sonuçları JSON'a kaydet (tüm alanları inceleyebilmen için)
    out_file = "test_results.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Ham sonuç kaydedildi: {out_file}")
    print("  Dosyayı aç, hangi alanların dolu/boş olduğunu gör, sonra tartışalım.")


if __name__ == "__main__":
    main()