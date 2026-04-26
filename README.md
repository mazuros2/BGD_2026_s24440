# BGD_2026_s24440

# 1. Wstępny problem projektu

Celem projektu jest analiza zgłoszeń serwisowych systemu 311 w Nowym Jorku z lat 2010–2019 w celu zbadania, które agencje miejskie (np. Policja, straż pożarna itp.) mają najdłuższy czas rozwiązywania spraw, jakie typy skarg dominują w poszczególnych dzielnicach miasta oraz jak liczba zgłoszeń zmieniała się na przestrzeni lat. Wyniki analizy mogą wspierać decyzje dotyczące alokacji zasobów w mieście oraz przyszłego zarządzania budżetem jednostek.

# 2. Data set
Dataset zawiera wszystkie zgłoszenia serwisowe skierowane do konkretnych agencji miejskich NYC. Każdy wiersz zawiera typ skargi, agencję obsługującą, lokalizację geograficzną, datę utworzenia i datę zamknięcia zgłoszenia.
Rozmiar datasetu to 10 GB ale na potrzeby tego projektu i skróciłem te dane do ok. 2GB za pomoca skryptu w Pythonie.
Link do datasetu: https://catalog.data.gov/dataset/311-service-requests-from-2010-to-2019

# 3. Ryzyka jakości danych

### 3.1 Współrzędne (0, 0) zamiast NULL
Część wierszy ma latitude = 0 i longitude = 0 zamiast pustej wartości przez co w przyszłych analizach może być problem z analizą geograficzną.

### 3.2 Złe daty spraw
Część wierszy ma closed_date wcześniejszy niż created_date, co ewidentnie jest błędem podczas wprowadzania danych.

### 3.3 Nieokreślone lokalizacje
Niektóre wiersze mają borough jako 'Unspecified' i brakuje kodu pocztowego. Te rekordy trafiają do faktu bez location_key, więc znikają ze wszystkich analiz geograficznych.


# 4. Diagram ERD

![diagram ERD](https://github.com/user-attachments/assets/38cbb934-c37f-4120-8e9f-5e46b1185bd8)

# 5. Architektura medalionowa

### Bronze — surowe dane
Dane skopiowane bezpośrednio z pliku CSV bez żadnych transformacji. Wszystkie kolumny przechowywane jako TEXT. 

### Silver — oczyszczone i otypowane
Dane są czyszczone, rzutowane na właściwe typy i walidowane przez PySpark. Ta warstwa usuwa duplikaty, rzutuje kolumny na właściwe typy, filtruje wiersze z błędnymi datami lub współrzędnymi i standaryzuje wielkość liter w tekstach.

### Gold - godtowe do analizy
Dane zostały rozdzielone do innych tabel aby ułatwić raporty i dashboardy.

# 6. Diagram pipeline'u

## Batch pipeline
<img width="751" height="152" alt="Batch-Pipeline" src="https://github.com/user-attachments/assets/8a226c45-4add-4970-9552-6970f1ce065e" />

## Kafka pipeline
<img width="1021" height="142" alt="Kafka-Pipeline" src="https://github.com/user-attachments/assets/3a7dd5a9-a15d-438b-b90e-ec097be607b7" />



## 7. Architektura pipeline'u

Pipeline jest orkiestrowany przez Apache Airflow przy użyciu DAGa. Liczba tasków zależy od wartości `FEATURE_FLAG` w pliku `.env`:
Tryb batch służy do jednorazowego załadowania dużego zbioru danych historycznych (np. cały CSV 3GB na raz). Tryb kafka służy do ładowania przyrostowego — dane trafiają do kolejki i pipeline uruchamia się gdy KafkaSensor wykryje nowe wiadomości. Oba tryby używają tego samego kodu silver i gold — zmienia się tylko źródło danych w warstwie bronze.

Zmiana trybu wymaga edycji `FEATURE_FLAG` w `.env` i restartu kontenerów (`docker-compose down && docker-compose up -d`).

Każdy task musi zakończyć się sukcesem zanim następny się uruchomi — w przeciwnym razie pipeline upada. DAG jest uruchamiany ręcznie przez użytkownika w Airflow UI.

### Task — kafka_produce _(tylko tryb kafka)_
Producer wczytuje plik CSV wiersz po wierszu i wysyła każdy wiersz jako wiadomość JSON do topicu `nyc_311_raw` w Apache Kafka. Klucz wiadomości to `unique_key` co gwarantuje że ten sam rekord zawsze trafia do tej samej partycji. Topic ma 3 partycje i retencję 7 dni.

### Task — wait_for_kafka_messages _(tylko tryb kafka)_
KafkaSensor nasłuchuje topicu `nyc_311_raw` i blokuje pipeline dopóki nie wykryje wiadomości. Sprawdza liczbę wiadomości w partycjach co 15 sekund. Dzięki temu pipeline nie przechodzi do bronze dopóki producent nie skończy wysyłać danych.

### Task — bronze_load
W trybie kafka PySpark czyta wiadomości z Kafki (`spark.read.format("kafka")`), automatycznie wykrywa schemat JSON z próbki 1000 wiadomości, rozpakuje JSON do kolumn i nadpisuje nazwy kolumn na ustandaryzowane. 

W trybie batch PySpark czyta CSV bezpośrednio z dysku. W obu trybach wszystkie 44 kolumny są przechowywane jako tekst bez żadnych transformacji.

### Task — silver_transform
PySpark czyta dane z `bronze.request_raw`, stosuje wszystkie transformacje czyszczące i typujące w pamięci RAM, a następnie zapisuje wynik do `silver.request_cleaned`.

### Task — gold_tables
PySpark buduje wszystkie pięć tabel wymiarów z `silver.request_cleaned` i zapisuje je do schematu gold: `gold.date`, `gold.agency`, `gold.location`, `gold.complaint` oraz `gold.status`. Tabela `gold.status` używa `ON CONFLICT DO NOTHING` aby uniknąć duplikatów przy kolejnych uruchomieniach pipeline'u.

### Task — gold_requests_table
PySpark czyta `silver.request_cleaned` oraz wszystkie pięć poprzednich stworzonych tabel, wykonuje LEFT JOINy aby rozwiązać klucze obce, oblicza `resolution_hours` i zapisuje `gold.requests`.

### Task — gold_aggregations
PySpark czyta tabelę faktów aby zbudować dwie tabele agregacji: `gold.aggregation_complaint_by_year` (trendy skarg w czasie) oraz `gold.aggregation_channel_by_borough` (kanały zgłoszeń na dzielnicę z procentowym udziałem).

## 8. Obrazy dockerowe
Postanowiłem wykorzystać obrazy dockerowe aby ułatwić uruchomienie projektu na różnych środowiskach bez konieczności pobierania PostgreSQL czy Apache Airflow (i też chciałem zadbać o pamięć na laptopie).
W projekcie jest `Dockerfile` i `docker-compose.yml`. Dockerfile bierze oficjalny obraz Airflow i dopisuje do niego instalację Javy, PySpark (`3.5.0`), kafka-python oraz sterowniki JDBC. docker-compose.yml buduje 6 kontenerów:
### Postgres — nyc_311_postgres
Główna baza danych projektu. Przechowuje dane we wszystkich trzech warstwach (bronze, silver, gold). Skrypty SQL z folderu `init_sql/` są wykonywane automatycznie przy pierwszym uruchomieniu, tworząc wszystkie schematy i tabele. Dostępny na porcie 5433.
### Airflow DB — nyc_311_airflow_db
Osobna baza danych używana wyłącznie przez Airflow do przechowywania własnych metadanych.
### Apache Kafka — nyc_311_kafka
Broker Kafki (`apache/kafka:3.7.0`) działający w trybie KRaft (bez zookeepera). Trzyma wiadomości w topicu `nyc_311_raw`. Dostępny wewnątrz sieci Docker pod adresem `kafka:9092` oraz pod `localhost:9094`.
### Kafka UI — nyc_311_kafka_ui
Interfejs webowy do podglądu Kafki (`http://localhost:8090`). Pokazuje topici, wiadomości w partycjach i offsety konsumentów. Przydatny do weryfikacji czy producent wysłał dane.
### Airflow Scheduler — nyc_311_airflow_scheduler
Uruchamia harmonogram zadań Airflow i monitoruje ich wykonanie. Odpowiada za uruchamianie tasków. Czeka na Kafkę przed startem. Czyta `FEATURE_FLAG` ze zmiennych środowiskowych aby zdecydować czy DAG zawiera taski Kafki czy nie.
### Airflow Webserver — nyc_311_airflow_webserver
Udostępnia interfejs użytkownika Airflow (`http://localhost:8080`), gdzie można monitorować DAGi, przeglądać logi i ręcznie uruchamiać taski.

# 9. Instalacja i uruchomienie projektu
### Wymagania:
- Docker
- GIT

1. Sklonuj repozytorium
2. Pobierz dataset z linku i umieść go w folderze data/
3. Stworz plik .env w katalogu głównym projektu z zawartością (przykładowe dane):
```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=nyc_311_raw
CSV_PATH=/opt/airflow/data/311_nyc_requests.csv
FEATURE_FLAG=kafka #2 flagi: batch / kafka
```
4. Uruchom docker-compose:
```
docker-compose up -d --build
```
5. Po instalacji obrazów i uruchomieniu kontenerów można je monitorować za pomocą:
```
docker-compose ps
```
6. Airflow webserver będzie dostępny pod adresem http://localhost:8080
7. Kafka UI jest dostępna pod adresem http://localhost:8090 i nie wymaga logowania 
8. Zaloguj się do Airflow (domyślne dane logowania to airflow/airflow) i znajdź DAG o nazwie "nyc_311_pipeline". Możesz go uruchomić ręcznie.
9. Po uruchomieniu pipeline'u możesz monitorować postęp poszczególnych tasków w interfejsie Airflow. Po zakończeniu pipeline'u dane będą dostępne w bazie danych PostgreSQL w schemacie gold do dalszej analizy i raportowania.
10. Aby zatrzymać kontenery, użyj:
```
docker-compose down
```
albo zeby zatrzymac i usunąć volumes
```
docker-compose down -v
```
