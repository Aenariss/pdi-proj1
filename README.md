## PDI Projekt
### Zpracování proudu dat ArcGIS Stream Services z dopravy IDSJMK pomocí Apache Spark
### Autor: Vojtěch Fiala \<xfiala61\>

Aplikace byla vyvíjena na Pythonu 3.10.12. Na ostatních verzích není funkčnost zaručena, ale neměl by teoreticky být problém.
Pro stáhnutí požadovaných knihoven je možné použít soubor `requirements.txt` s využitím PIPu, pip install -r requirements.txt

Vývoj aplikace provázela spousta technických problémů, které musely být řešeny nestandardním způsobem - např. nebylo možné číst websocket přímo ze Sparku,
ale bylo nutné vytvořit si vlastní server, který websocket čte a následne jeho data přeposílá na localhost, ze kterého poté spark čte.
Tento server je implementovaný v src/redirectToLocalhost.py.


Aplikaci je možné spustit za pomoci souboru ./run.sh s vhodnými argumenty.
**DŮLEŽITÉ - PO KAŽDÉM UKONČENÍ APLIKACE JE NUTNÉ SPUSTIT ./clean.sh KTERÝ ZABIJE BĚŽÍCÍ INSTANCI SERVERU**

Aplikace nabízí 6 možných funkcí (viz zadání). Funkci, kterou má aplikace vykonávat, je možné zvolit parametrem --mode | -m.
Jednotlivé možnosti jsou:

* --mode north -> průběžně vypisuje vozidla mířící na sever.
* --mode trains -> vypisuje seznam vlaků s ID jejich poslední hlášené zastávky a časem poslední aktualizace od startu aplikace.
* --mode mostdelayed -> Vypisuje 5 nejvíce zpožděných vozů od startu aplikace.
* --mode delayed3min -> Vypisuje 5 zpožděných vozů od nejčerstvěji hlášeného.
* --mode avgdelay -> Vypisuje průměrné zpoždění všech vozů za poslední 3 minuty.
* --mode avganntime -> Vypisuje průměrnou dobu mezi hlášeními, kterou počítá z 10 nejnovějších hlášení.

Pro spuštění testů je možné využít parametr --test | -t. Více viz TESTING.md

Pokud mají být data čtena "živě", stačí vynechat parametr --test a použít `./run.sh --mode <mode>`.

Použité knihovny mimo standardní jsou:
websockets - licencováno pod BSD License - https://pypi.org/project/websockets/.
pyspark - licencováno pod Apache Software License - https://pypi.org/project/pyspark/.

# todo vyzkouset, ze nejaka example aplikace na clusteru funguje s vyuzitim netcatu, jeslti vubec neco fungovat bude (s live vystupem)
# vyzkouset ze vubec funguje normalne ta appka, ne jen na clsuteru -- NEFUNGUJE, neni to moej chyba a odevzdavam jak to je :relieved:
# zkusit udelat standalone cluster na Mintu a otestovat tam