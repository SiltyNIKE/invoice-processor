# Krátky návod na použitie

## Čo je potrebné pripraviť

- Service account credentials (`credentials.json`)
- Gemini API kľúč
- Google Sheets ID
- ID priečinkov v Google Drive (ToProcess, Extracted, Errors)

Všetky konfiguračné hodnoty nastavte v súbore `.env`.

## Súbory v repozitári

Plánované súbory na odovzdanie:

- `invoice_processor.py`
- `.env`
- `requirements.txt`

## Výsledok behu

Výstupná tabuľka (dôkaz, že skript bežal):

https://docs.google.com/spreadsheets/d/1j7iH4-nMccZHlySyUeGJLZITiOMwyWO_KwupIzhzG84/edit?gid=652310833#gid=652310833

Skript som pre Vás testoval na 10 PDF súboroch, ktoré boli dodané na pohovore.

## Aktuálny stav

Skript ešte potrebuje dopracovanie, pretože sa objavujú falošné presuny do `Errors` (false positives).
Pravdepodobne bude potrebné doladiť prompt alebo zmeniť model; počas testu sa používal Gemini Flash 2.5.

## Aktualizácia 20.03.2026 (changelog)

- [Critical] Opravená syntaktická chyba v `process_file`, skript sa znovu korektne spustí v režime watcher.
- [Critical] Prebudovaný routing decision tree tak, aby **všetky vetvy** (full / rename-only / errors / quarantine) vždy skončili zápisom do `DocsProcessed` presne raz.
- [Critical] Doplnené robustné error handling vetvy pre PDF/LLM/Drive/Sheets chyby bez prerušenia behu watcher-a pri zlyhaní jedného dokumentu.
- [Critical] Doplnené chýbajúce súbory `.env.example` a `SETUP.md` pre čisté nasadenie projektu.
- [Major] Zjednotený naming pattern na `FA_YYYYMMDD_supplier_SID_id` s underscore separátormi a presnou štruktúrou bez redundantných znakov.
- [Major] Zladený LLM prompt a field mapping na kanonické kľúče špecifikácie (vrátane alias remap pre spätnú kompatibilitu).
- [Major] Opravené hlavičky a poradie stĺpcov pre `InvoiceItemsList` a `DocsProcessed` podľa špecifikácie (operatívne polia na konci).
- [Major] Do `requirements.txt` doplnené `pytesseract` a `pdf2image` pre OCR fallback v čistom prostredí.
- [Major] Pridaná OCR fallback logika pre naskenované PDF bez textovej vrstvy.
- [Major] Optimalizované logovanie do tabuliek s rozdelením plnej extrakcie a funkcie iba na premenovanie.

## Aktualizácia 19.03.2026 (changelog)

- Zjednotené názvy extrahovaných polí podľa požadovanej schémy, vrátane mapovania starších aliasov.
- Sprísnená validácia výstupu z LLM: oddelené kontroly pre premenovanie dokumentu a pre zápis položiek do tabuľky.
- Doplnené robustnejšie spracovanie čísel, dátumov a mien dodávateľov pre konzistentné premenovanie súborov.
- Upravená logika presunov dokumentov (Extracted / Errors / Quarantine) s jasnejším dôvodom rozhodnutia.
- Rozšírené logovanie do `DocsProcessed` o stav spracovania, čas a chybový dôvod pre lepšiu auditovateľnosť.
- Posilnená idempotencia spracovania dokumentov (detekcia zmenených súborov podľa metadát).
- Pripravený workflow pre uzatvorenie faktúr (Invoices Closed) cez interné číslo z tabuľky.