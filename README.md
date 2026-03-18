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