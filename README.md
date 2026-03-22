# BGD_2026_s24440

# 1. Wstępny problem projektu

Celem projektu jest analiza zgłoszeń serwisowych systemu 311 w Nowym Jorku z lat 2010–2019 w celu zbadania, które agencje miejskie (np. Policja, straż pożarna itp.) mają najdłuższy czas rozwiązywania spraw, jakie typy skarg dominują w poszczególnych dzielnicach miasta oraz jak liczba zgłoszeń zmieniała się na przestrzeni lat. Wyniki analizy mogą wspierać decyzje dotyczące alokacji zasobów w mieście oraz przyszłego zarządzania budżetem jednostek.

# 2. Ryzyka jakości danych

2.1 Współrzędne (0, 0) zamiast NULL
Część wierszy ma latitude = 0 i longitude = 0 zamiast pustej wartości przez co w przyszłych analizach może być problem z analizą geograficzną.

2.2 Złe daty spraw
Część wierszy ma closed_date wcześniejszy niż created_date, co ewidentnie jest błędem podczas wprowadzania danych.

2.3 Nieokreślone lokalizacje
Niektóre wiersze mają borough jako 'Unspecified' i brakuje kodu pocztowego. Te rekordy trafiają do faktu bez location_key, więc znikają ze wszystkich analiz geograficznych.


# 3. Diagram ERD

![diagram ERD](https://github.com/user-attachments/assets/38cbb934-c37f-4120-8e9f-5e46b1185bd8)
