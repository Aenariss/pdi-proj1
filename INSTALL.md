## PDI Projekt
### Zpracování proudu dat ArcGIS Stream Services z dopravy IDSJMK pomocí Apache Spark -- Instalace
### Autor: Vojtěch Fiala \<xfiala61\>

Pro používání je nutné mít nainstalovány požadované knihovny vypsané v requirements.txt, lze nainstalovat např. přes pip install -r requirements.txt.

### Přimé spouštění
Přímé spouštění je popsáno v README včetně vysvětlení parametrů. Pro ukázku spouštění nad streamovanými daty:

`./run.sh --mode <mode>`

kde mode je popsaný v README. 

**DŮLEŽITÉ - PO KAŽDÉM UKONČENÍ APLIKACE JE NUTNÉ SPUSTIT ./clean.sh KTERÝ ZABIJE BĚŽÍCÍ INSTANCI SERVERU**

### Nasazení na cluster
Jakmile je cluster inicializovaný, stačí použít
./bin/spark-submit src/dataDownloader.py --mode \<mode\>

kde ./bin/spark-submit je možné nahradit jiným odkazem -- tento předpokládá, že je projekt spuštěn ve virtuálním prostředí a složky jsou v kořenovém adresáři.
Je taky možné případně parametrem --host \<host\> manuálně nastavit adresu lokálního websocket redirect serveru.

Server pro redirect websocketu je zároveň nutné manuálně spustit - to je možné udělat pomocí 
`python3 ./src/redirectToLocalhost &`.

S každým novým spuštěním aplikace je nutné redirect server restartovat, pro vypnutí slouží `./clean.sh` a je pak nutné ho spustit znova.