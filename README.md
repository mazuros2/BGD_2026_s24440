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

![pipeline](https://github.com/user-attachments/assets/79dbceca-3d86-4fd2-b0a1-9a5ae5824a2e)

# 7. Architektura pipeline'u
Pipeline jest orkiestrowany przez Apache Airflow przy użyciu DAGa z pięcioma kolejnymi taskami. Każdy task musi zakończyć się sukcesem zanim następny się uruchomi w przeciwnym razie pipeline upada. Taski są następujące:
### Task 1 — bronze_load
PySpark wczytuje plik CSV i zapisuje wszystkie wiersze do bronze.request_raw bez żadnych transformacji. Wszystkie 44 kolumny są przechowywane jako tekst. Task używa mode("append") — baza danych automatycznie odrzuca duplikaty jeśli unique_key jest kluczem głównym.
### Task 2 — silver_transform
PySpark czyta dane z bronze.request_raw, stosuje wszystkie transformacje czyszczące i typujące w pamięci RAM, a następnie zapisuje wynik do silver.request_cleaned.
### Task 3 — gold_tables
PySpark buduje wszystkie pięć tabel wymiarów z silver.request_cleaned i zapisuje je do schematu gold: gold.date, gold.agency, gold.location, gold.complaint oraz gold.status. Tabela gold.status używa ON CONFLICT DO NOTHING aby uniknąć duplikatów przy kolejnych uruchomieniach pipeline'u.
### Task 4 — gold_requests_table
PySpark czyta silver.request_cleaned oraz wszystkie pięć poprzednich stworzonych tabel, wykonuje LEFT JOINy aby rozwiązać klucze obce, oblicza resolution_hours i zapisuje gold.requests.
### Task 5 — gold_aggregations
PySpark czyta tabelę aby zbudować dwie tabele agregacji: gold.aggregation_complaint_by_year (trendy skarg w czasie) oraz gold.aggregation_channel_by_borough (kanały zgłoszeń na dzielnica z procentowym udziałem).

# 8. Obrazy dockerowe
Postanowiłem wykorzystać obrazy dockerowe aby ułatwić uruchomienie projektu na róźnych środowiskach bez koniecznosci  pobierania PostgreSQL czy Apache Airflow (i też chciałem zadbać o pamięć na laptopie).
W projekcie jest Dockerfile i Docker-compose.yml.
Dockerfile bierze oficjalny obraz Airflow i dopisuje do niego instalację Javy i PySpark.
docker-compose.yml buduje 4 kontenery: 
### Postgres - nyc_311_postgres
Główna baza danych projektu. Przechowuje dane we wszystkich trzech warstwach (bronze, silver, gold). Skrypty SQL z folderu init_sql/ są wykonywane automatycznie przy pierwszym uruchomieniu, tworząc wszystkie schematy i tabele.
### Airflow db - nyc_311_airflow_db 
Osobna baza danych używana wyłącznie przez Airflow do przechowywania własnych metadanych
### Airflow scheduler - nyc_311_airflow_scheduler
Uruchamia harmonogram zadań Airflow i monitoruje ich wykonanie. Odpowiada za uruchamianie tasków.
### Airflow webserver - nyc_311_airflow_webserver
Udostępnia interfejs użytkownika Airflow, gdzie można monitorować DAGi, przeglądać logi i ręcznie uruchamiać taski. Jest dostępny na porcie 8080.

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
7. Zaloguj się do Airflow (domyślne dane logowania to airflow/airflow) i znajdź DAG o nazwie "nyc_311_pipeline". Możesz go uruchomić ręcznie.
8. Po uruchomieniu pipeline'u możesz monitorować postęp poszczególnych tasków w interfejsie Airflow. Po zakończeniu pipeline'u dane będą dostępne w bazie danych PostgreSQL w schemacie gold do dalszej analizy i raportowania.
9. Aby zatrzymać kontenery, użyj:
```
docker-compose down
```
albo zeby zatrzymac i usunąć volumes
```
docker-compose down -v
```
