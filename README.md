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

Zatim je potrebno pokrenuti naredbu:
```
docker-compose up --build
```

Nakon što se sve pokrene, u novom terminalu je potrebno pokrenuti Python skriptu za izvođenje ETL procesa:
```
python '.\Apache Spark\run_scripts.py'
```
