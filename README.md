## Upute za korištenje
<hr>
Za pokretanje potrebno je preuzeti zip ili klonirati repozitorij s:

```
git clone https://github.com/T30M47/PrakticniRad.git
```

Nakon kloniranja ili preuzimanja zip datoteke, potrebno je prebaciti se u terminal na lokaciju korijenskog direktorija (gdje se nalazi Dockerfile):
```
cd ./PrakticniRad
```

Prije pokretanja kontejnera, potrebno je lokalno instalirati psycopg2-binary paket kako bi se moglo pokrenuti ETL skripte:
```
pip install psycopg2-binary
```

Zatim je potrebno pokrenuti naredbu:
```
docker-compose up --build
```

Nakon što se sve pokrene, u novom terminalu je potrebno pokrenuti Python skriptu za izvođenje ETL procesa i pokretanje Dash web aplikacije (pazite da ste i dalje u korijenskom direktoriju gdje se nalazi i Dockerfile):
```
python 'Apache Spark\run_scripts.py'
ili
python Apache\ Spark/run_scripts.py 
```

Pokretanje ETL skripti i web aplikacije mogu potrajati dosta dugo, a nakon što ste sve pokrenuli, web aplikacija postaje dostupna na poveznici:
```
http://localhost:8050
```

Ako želite isprobati dodatne upite nad skladištem, upute su dane u datoteci upute.txt.
