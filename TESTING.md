## PDI Projekt
### Zpracování proudu dat ArcGIS Stream Services z dopravy IDSJMK pomocí Apache Spark -- Testování
### Autor: Vojtěch Fiala \<xfiala61\>

Jak je popsáno už v README, ke spouštění testů slouží parametr --test | -t.

Testy vycházejí ze souborů ve složce tests/, přičemž vypíšou získaný výsledek a zároveň očekáváný výsledek. Nejprve je vypsán výsledný dataFrame přes metodu *.show()* a poté očekávané hodnoty.
Soubory ve složce tests/ vycházejí z reálně zachycené komunikace (uložena v tests/default.json), ze které byly vybírány menší kusy dat, které byly dále upraveny pro účely testů.
Testy je možné spouštět i přímo s využitím `python3 ./src/dataDownloader.py --test --mode <mode>` a není potřeba používat *run.sh*, který spouští i server.

Parametr *--mode* určuje, který test se spustí a odpovídá parametrům použitým pro normální spuštění, viz README.md

Co se týče jednotlivých testů, tak ty testují:

* --mode north -> Test, že jsou načtena pouze data *isinactive == false* a zároveň, že soubor, který obsahuje pouze 2 aktivní vozidla mířící na sever, jsou obě tato vozdila vypsána.
* --mode trains -> Test, že soubor obsahující více záznamů pro jeden vlak (simulující více dat od startu aplikace) načte pouze nejnovější záznam pro každý vlak a tedy i odpovídající nejnovější poslední zastávku.
* --mode mostdelayed -> Test, že soubor obsahující více zpoždění správně vypíše pro každé vozidlo pouze to poslední hlášené zpoždění a výpíše je od největšího zpoždění po nejmenší. Data byla upravena, ať jsou vypsaná zpoždění jasně seřazena - zpoždění 50,49,48,47,46 včetně odpovídajících ID a času posledního hlášení.
* --mode delayed3min -> Test, že jsou správně načítána pouze data za poslední 3 minuty a pro každé vozidlo pouze to nejnovější hlášení (kdy je jako "aktuální" čas braný čas nejnovějšího hlášení), přičemž soubor obsahuje hodnot více, ale pouze některé z nich jsou za poslední 3 minuty -- konkrétně pouze 3 hodnoty.
* --mode avgdelay -> Test, že jsou správně počítána průměrována data pouze za poslední 3 minuty (a pro každé vozidlo pouze nejaktuálnější zpoždění)
* --mode avganntime -> Test, že jsou pro 10 nejnovějších záznamů jednotlivých vozidel brány v potaz pouze poslední 2 nejnovější záznamy, ze kterých je počítán průměrný čas mezi nimi.
