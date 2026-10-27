# PyFlow

Biblioteka do definiowania i uruchamiania przepływów danych (DAG).

## Instalacja
Pobierz repozytorium, stwórz środowisko wirtualne i zainstaluj w trybie edytowalnym:
\`\`\`bash
python -m venv .venv
source .venv/bin/activate  # (Linux/Mac) lub .venv\Scripts\activate (Windows)
pip install -e .
pip install pytest
\`\`\`

## Testy
Uruchom testy jednostkowe poleceniem:
\`\`\`bash
python -m pytest
\`\`\`

## Przykład użycia
\`\`\`python
from pyflow import Task, Workflow, execution_layers

def dummy(inputs, params): pass

wf = Workflow()
wf.add_task(Task("extract", dummy))
wf.add_task(Task("transform", dummy, dependencies=["extract"]))

wf.validate()
print(execution_layers(wf)) # Wypisze: [['extract'], ['transform']]
\`\`\`

## Algorytm
Do sortowania topologicznego i wyznaczania warstw użyto **algorytmu Kahna**. 
- Złożoność czasowa: `O(V + E)` (gdzie V to liczba zadań, E to liczba zależności).
- Dlaczego? Algorytm Kahna idealnie pasuje do naturalnego grupowania wierzchołków w warstwy wykonania, grupując wierzchołki o stopniu wejścia równym zero. Remisy rozwiązywane są alfabetycznie.