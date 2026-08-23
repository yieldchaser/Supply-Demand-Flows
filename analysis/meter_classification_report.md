# Blue Tide Meter Classification Report

*Generated:* `2026-08-23T17:20:41.804608+00:00` — deterministic output of `scripts/classify_meters.py` over `data/curated/*.parquet`.

**812 meters classified across 5 EBB sources.** One primary class and one confidence per meter; evidence strings are carried in `config/meters/classification.json`.

## Class distribution

| Source | unknown | ldc | industrial | interconnect | basin_egress | lng_export | system | storage | power_burn | hub | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **gulf_south** | 238 | 161 | 120 | 83 | 37 | 4 | 23 | 24 | 24 | 3 | 717 |
| **gasnom** | 46 | 1 | 5 | 2 | 0 | 7 | 0 | 0 | 0 | 0 | 61 |
| **quorum** | 1 | 0 | 0 | 3 | 0 | 5 | 2 | 0 | 0 | 0 | 11 |
| **bhe** | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 0 | 0 | 1 |
| **cheniere** | 2 | 0 | 0 | 0 | 0 | 20 | 0 | 0 | 0 | 0 | 22 |
| **all** | **287** | **162** | **125** | **88** | **37** | **37** | **25** | **24** | **24** | **3** | **812** |

## Confidence distribution

| Source | high | unknown | medium |
|---|---|---|---|
| **gulf_south** | 361 | 238 | 118 |
| **gasnom** | 11 | 46 | 4 |
| **quorum** | 6 | 1 | 4 |
| **bhe** | 1 | 0 | 0 |
| **cheniere** | 4 | 2 | 16 |
| **all** | **383** | **287** | **142** |

## Storage inventory (24 meters)

Feeds the EIA weekly-print nowcast. Capacity/operator where researchable (citations inline in classification.json evidence). Petal/Bistineau/Jackson are Gulf South's own fields; the '(Petal ...)' suffixed points are lateral counterparty points at those facilities.

| Source | Loc | Name | Flow | Mean Dth/d | Max Dth/d | Zero % | Conf | Facility/Operator/Capacity | Basin |
|---|---|---|---|---|---|---|---|---|---|
| gulf_south | 50202 | Petal Pipeline Injection/Withdrawal | B | 198,016 | 694,948 | 0% | high | Petal (Boardwalk / Gulf South) · 29.6 Bcf WG · net | - |
| gulf_south | 22806 | Bistineau Injection Expansion | R | 90,378 | 199,605 | 0% | high | Bistineau (Boardwalk / Gulf South) · 78.0 Bcf WG · gross | - |
| gulf_south | 50201 | Petal Storage Injection/Withdrawal | B | 70,957 | 192,425 | 0% | high | Petal (Boardwalk / Gulf South) · 29.6 Bcf WG · net | - |
| gulf_south | 10401 | Bistineau Injection | R | 55,460 | 111,469 | 0% | high | Bistineau (Boardwalk / Gulf South) · 78.0 Bcf WG · gross | - |
| gulf_south | 10402 | Bistineau Withdrawal | D | 30,906 | 93,888 | 0% | high | - | - |
| gulf_south | 22807 | Bistineau Withdrawal Expansion | D | 27,929 | 108,361 | 1% | high | - | - |
| gulf_south | 23358 | Enstor Katy Storage | ? | 25,064 | 414,765 | 92% | high | Katy (Enstor) · 23.5 Bcf WG · net | - |
| gulf_south | 23351 | Tres Palacios Storage | ? | 21,648 | 122,480 | 16% | high | Tres Palacios (Kinder Morgan (Enbridge expansion)) · 38.4 Bcf WG · net | - |
| gulf_south | 23361 | Bobcat Storage | ? | 17,292 | 40,940 | 1% | high | Bobcat (Port Barre Investments / Enbridge) · 20.5 Bcf WG · net | - |
| gulf_south | 23375 | Gulf South Leg (Petal Storage) | ? | 9,057 | 70,095 | 28% | high | - | - |
| gulf_south | 23362 | Sesh (Petal Storage) | ? | 6,633 | 35,000 | 71% | high | - | - |
| gulf_south | 23374 | Petal Gas Storage (Gulf South Leg) | ? | 6,247 | 53,390 | 70% | high | - | - |
| gulf_south | 23352 | Bay Gas Storage @ Axis | ? | 4,954 | 45,421 | 64% | high | - | - |
| gulf_south | 23380 | BBT Mississippi  (Petal  Storage) | ? | 2,232 | 20,000 | 83% | high | - | - |
| gulf_south | 23356 | Jefferson Island Storage | ? | 1,265 | 43,822 | 60% | high | Jefferson Island (Enstor) | - |
| gulf_south | 23377 | Bistineau Storage (Enable) | ? | 728 | 8,997 | 87% | high | - | - |
| gulf_south | 23369 | Tennessee Gas (Petal Storage) | ? | 397 | 19,527 | 57% | high | - | - |
| gulf_south | 23601 | Jackson Storage Injection | R | 208 | 2,689 | 92% | high | Jackson (Boardwalk / Gulf South) · 13.5 Bcf WG · gross | - |
| gulf_south | 23360 | Arcadia Gas Storage | ? | 150 | 13,196 | 98% | high | - | - |
| gulf_south | 24346 | Tooke Well W/D | ? | 40 | 90 | 2% | high | - | - |
| gulf_south | 23353 | Bay Gas Storage @ Whistler Junction | ? | 0 | 0 | 100% | medium | - | - |
| gulf_south | 23357 | Napoleonville Storage (Bridgeline) | ? | 0 | 0 | 100% | medium | - | - |
| gulf_south | 23378 | Leaf River Storage | ? | 0 | 0 | 100% | medium | - | - |
| gulf_south | 23602 | Jackson Storage Withdrawal | D | 0 | 0 | 100% | medium | - | - |

## Basin-egress inventory (37 meters)

Haynesville-attributed takeaways and producer/gatherer receipts. All researched high-confidence entries cite public sources; gatherer-name matches are medium.

| Source | Loc | Name | Flow | Mean Dth/d | Max Dth/d | Zero % | Conf | Facility/Operator/Capacity | Basin |
|---|---|---|---|---|---|---|---|---|---|
| gulf_south | 22108 | Rock Springs/Scott Mtn (To Transco 85) | D | 1,127,243 | 1,255,712 | 0% | high | - | haynesville |
| gulf_south | 24421 | Bennington (From Midship) | R | 656,303 | 799,189 | 0% | medium | - | haynesville |
| gulf_south | 3362 | Lonewa (To Texas Gas) | D | 550,955 | 656,487 | 0% | high | - | haynesville |
| gulf_south | 22329 | Sherman (From Enterprise) | R | 443,967 | 551,991 | 0% | medium | - | haynesville |
| gulf_south | 24469 | Bland Lake-Kudu | ? | 425,412 | 505,721 | 0% | high | - | haynesville |
| gulf_south | 26016 | Bbt Trans-Union Claiborne Parish | ? | 253,436 | 321,437 | 0% | high | - | haynesville |
| gulf_south | 22492 | Bennington (From Mark West) | R | 203,872 | 297,229 | 0% | medium | - | haynesville |
| gulf_south | 22410 | Plantation West Cp (Kinderhawk) | ? | 199,924 | 302,902 | 0% | medium | - | haynesville |
| gulf_south | 22330 | Bennington (From Enable Ok) | R | 199,859 | 325,224 | 0% | medium | - | haynesville |
| gulf_south | 22561 | Plantation West Ii - Expansion | ? | 186,276 | 359,023 | 0% | medium | - | haynesville |
| gulf_south | 21921 | Midcoast - Carthage (Expansion) | ? | 183,530 | 264,914 | 0% | medium | - | haynesville |
| gulf_south | 22631 | Magnolia Cdp Ii - Expansion | ? | 179,122 | 233,822 | 0% | medium | - | haynesville |
| gulf_south | 24245 | Markwest Carthage [Expansion] | ? | 167,038 | 249,031 | 0% | medium | - | haynesville |
| gulf_south | 22110 | Gulf Run Delhi | ? | 154,823 | 234,393 | 0% | high | - | haynesville |
| gulf_south | 22708 | Bulldog Panola (Bta Egt Gathering) | ? | 144,563 | 316,400 | 0% | high | - | haynesville |
| gulf_south | 22129 | Tennessee Heidelburg (Expansion) | ? | 109,237 | 173,416 | 1% | medium | - | haynesville |
| gulf_south | 21805 | Discovery Gas Transmission | ? | 104,167 | 150,883 | 0% | high | - | haynesville |
| gulf_south | 24446 | Gemini Panola County Tx (Expansion) | ? | 102,213 | 168,653 | 0% | medium | - | haynesville |
| gulf_south | 22647 | Hall Summit Cp - Qep  (Expansion) | ? | 97,706 | 142,833 | 0% | medium | - | haynesville |
| gulf_south | 22810 | Wharton (From Enterprise Texas) | R | 96,016 | 157,580 | 0% | medium | - | haynesville |
| gulf_south | 23373 | Transco (Petal Pipeline) | ? | 86,073 | 274,136 | 0% | high | - | haynesville |
| gulf_south | 21922 | Energy Transfer - Carthage (Expansion) | ? | 79,132 | 151,951 | 0% | medium | - | haynesville |
| gulf_south | 22662 | Holly Field Cp - Exco (Expanson) | ? | 73,106 | 172,319 | 0% | medium | - | haynesville |
| gulf_south | 21416 | Section 23 Cp - Aethon United | ? | 71,207 | 91,777 | 0% | medium | - | haynesville |
| gulf_south | 22653 | Desoto Parish (Etc Field Services) | ? | 58,735 | 100,246 | 0% | medium | - | haynesville |
| gulf_south | 24494 | Tristate Longview (From Tristate) | R | 51,538 | 71,484 | 0% | medium | - | haynesville |
| gulf_south | 9332 | West Monroe (From Enable) | R | 50,095 | 59,619 | 0% | medium | - | haynesville |
| gulf_south | 24454 | Amp Ii Etx  Panola (Expansion) | ? | 45,212 | 57,371 | 0% | medium | - | haynesville |
| gulf_south | 22382 | Thornlake Aethon | ? | 43,053 | 80,343 | 1% | medium | - | haynesville |
| gulf_south | 24424 | Aethon Hwy 5 (Expansion) | ? | 34,342 | 74,057 | 0% | medium | - | haynesville |
| gulf_south | 24362 | Momentum Midstream (M5 Desoto) | ? | 30,086 | 68,326 | 0% | medium | - | haynesville |
| gulf_south | 22171 | Bta Plant Rec - Carthage (Expansion) | R | 24,188 | 64,308 | 0% | medium | - | haynesville |
| gulf_south | 21532 | Ibex Koran Cp | ? | 24,171 | 58,808 | 4% | medium | - | haynesville |
| gulf_south | 26108 | GEP Haynesville II, LLC | ? | 23,860 | 29,000 | 1% | medium | - | haynesville |
| gulf_south | 24501 | Sponte Cp Panola County | ? | 21,088 | 25,057 | 0% | medium | - | haynesville |
| gulf_south | 21923 | Enterprise - Carthage (Expansion) | ? | 11,665 | 45,447 | 50% | medium | - | haynesville |
| gulf_south | 22636 | Logansport Cp 1 | ? | 10,216 | 11,000 | 0% | medium | - | haynesville |

## Top 25 meters by mean scheduled volume

| Mean Dth/d | Source | Loc | Name | Class | Conf |
|---|---|---|---|---|---|
| 2,374,322 | cheniere | CC200221 | CC200221-CORPUS CHRISTI-CCLIQ-D | lng_export | high |
| 2,219,301 | quorum | vgpqd | VENTURE GLOBAL PLAQUEMINES LNG DELIVERY | lng_export | high |
| 1,685,634 | gulf_south | 23201 | Perryville Transportation Point | hub | high |
| 1,479,590 | quorum | tgp | TGP/GXP GATOR EXPRESS | lng_export | high |
| 1,450,764 | cheniere | CT200111 | CT200111-CREOLE TRAIL-SPLIQ-D | lng_export | high |
| 1,443,640 | gasnom | 772300 | Cameron LNG (Del) | lng_export | high |
| 1,253,307 | quorum | vgcpd | VENTURE GLOBAL CALCASIEU PASS DELIVERY | lng_export | high |
| 1,127,243 | gulf_south | 22108 | Rock Springs/Scott Mtn (To Transco 85) | basin_egress | high |
| 1,055,938 | gulf_south | 24329 | Stratton Ridge (To Freeport Lng) | lng_export | high |
| 1,010,149 | cheniere | CC121073 | CC121073-KM TEJAS-SINTON-R | lng_export | high |
| 849,096 | gasnom | 772298 | TENN-CIP | lng_export | high |
| 821,885 | quorum | tetco | TETCO/GXP GATOR EXPRESS | lng_export | high |
| 695,428 | quorum | anr | ANR/TCPL MERMENTAU RIVER | interconnect | medium |
| 656,303 | gulf_south | 24421 | Bennington (From Midship) | basin_egress | medium |
| 644,423 | cheniere | CT109461 | CT109461-GILLIS-LEAP-R | lng_export | medium |
| 624,165 | gasnom | 805469 | Gulf Run | lng_export | high |
| 605,000 | gulf_south | 23376 | Coastal Bend (Enterprise) | lng_export | high |
| 550,955 | gulf_south | 3362 | Lonewa (To Texas Gas) | basin_egress | high |
| 550,151 | quorum | cgt | CGT/GXP GATOR EXPRESS | lng_export | high |
| 543,676 | cheniere | CT109471 | CT109471-GILLIS-ACADIAN-R | lng_export | medium |
| 541,280 | gulf_south | 23039 | Perryville Exchange Point | hub | high |
| 536,223 | gasnom | 287439 | LRC/Bridgeline - HH | interconnect | high |
| 460,443 | gasnom | 278925 | TransCameron Pipeline | lng_export | high |
| 454,058 | quorum | ttc | TTC/TCPL OAK GROVE CAMERON PARISH LA | interconnect | medium |
| 443,967 | gulf_south | 22329 | Sherman (From Enterprise) | basin_egress | medium |

## High-volume unknowns needing manual review (2)

Mean > 100,000 Dth/d with no rule/research coverage. These carry real 
analytical weight and deserve a human pass:

| Mean Dth/d | Source | Loc | Name | Current evidence |
|---|---|---|---|---|
| 279,657 | gulf_south | 24229 | George Co Seme (Merrill To Fgt) | no name-pattern or research signal matched (name='George Co Seme (Merr |
| 119,072 | gasnom | 772306 | CIP-LEG | no name-pattern or research signal matched (name='CIP-LEG', flow=?, me |

## lng_export cross-check vs existing seed maps

Sum of mean TSQ over meters classified lng_export, compared against the 
high-confidence entries in the pre-existing `config/meters/{gulf_south → 
lng_meter_map.json}, gasnom.json, quorum.json, bhe.json, cheniere.json`:

| Source | n lng_export | Σ mean Dth/d (high conf) | Σ mean Dth/d (all) | Seed-map agreement |
|---|---|---|---|---|
| gulf_south | 4 | 2,301,006 | 2,301,006 | ➕ extends seeds by 3 |
| gasnom | 7 | 3,739,954 | 4,028,123 | ➕ extends seeds by 6 |
| quorum | 5 | 6,324,234 | 6,324,234 | ➕ extends seeds by 4 |
| bhe | 1 | 3,243 | 3,243 | ✅ superset of seeds |
| cheniere | 20 | 4,844,545 | 7,662,246 | ➕ extends seeds by 18 |

Notes on intentional extensions beyond the seed maps (each carries cited 
evidence in classification.json): Cameron Interstate's TENN-CIP/TETCO-CIP/
CIP receipts and Gulf Run (Golden Pass supply), TransCameron (Cameron LNG), 
the Coastal Bend Header trio (Freeport LNG), Gator Express TGP/TETCO/CGT 
(Plaquemines), Cheniere CTPL/CCPL receipt sets, and Cove Point via EGTS. 
The Cameron LNG (Rec) and Sabine Pass LNG Rec points are terminal return/
placeholder legs that post zero; they classify as lng_export by name but 
carry no volume and must be excluded from feedgas sums.

