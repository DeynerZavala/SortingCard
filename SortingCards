import math
import random
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

###############################################################################
#                           FUNCIONES AUXILIARES                               #
###############################################################################

def card_sort_key(card):
    """
    Función de clave (key) para ordenar las cartas.
    Devuelve la 'prioridad' según (palo, valor).
    Se emplea en 'sorted' y en nuestro merge sort.
    """
    return card.key()

def merge(left, right, key=lambda x: x):
    """
    Combina dos listas ordenadas (left, right) en una sola lista ordenada.
    Usa la función 'key' para comparar los elementos.
    
    Parámetros:
        - left: lista (ya ordenada) con la mitad izquierda
        - right: lista (ya ordenada) con la mitad derecha
        - key:  función para obtener la clave de orden de cada elemento

    Retorna:
        - merged: lista que resulta de mezclar ambas en orden ascendente.
    """
    merged = []
    i, j = 0, 0
    
    while i < len(left) and j < len(right):
        if key(left[i]) <= key(right[j]):
            merged.append(left[i])
            i += 1
        else:
            merged.append(right[j])
            j += 1
    
    merged.extend(left[i:])
    merged.extend(right[j:])
    return merged

def parallel_merge_sort(array, key=lambda x: x, max_depth=2):
    """
    Implementa un merge sort que, en lugar de ser totalmente secuencial,
    hace llamadas recursivas en paralelo (usando ProcessPoolExecutor).
    
    Parámetros:
        - array: lista de elementos a ordenar
        - key: función para extraer la clave de cada elemento
        - max_depth: controla la profundidad máxima de paralelismo.
                     Si max_depth <= 0, se hace un sorted secuencial.
                     
    Retorna:
        - La lista 'array' ordenada (por la función key).
    """
    if len(array) <= 1 or max_depth <= 0:
        return sorted(array, key=key)
    
    mid = len(array) // 2
    left_part = array[:mid]
    right_part = array[mid:]
    
    # Creamos un pool de procesos para ejecutar las mitades en paralelo
    with ProcessPoolExecutor() as executor:
        left_future = executor.submit(parallel_merge_sort, left_part, key, max_depth - 1)
        right_future = executor.submit(parallel_merge_sort, right_part, key, max_depth - 1)
        
        left_sorted = left_future.result()
        right_sorted = right_future.result()
        
    return merge(left_sorted, right_sorted, key=key)

def get_optimal_max_depth():
    """
    Retorna un valor aproximado de max_depth
    basado en el número de núcleos de la CPU.
    La idea es no subdividir más allá de ~log2(#Cores).
    """
    cpu_count = multiprocessing.cpu_count()
    if cpu_count < 2:
        return 0
    return int(math.log2(cpu_count))

def print_deck_with_duplicates(sorted_deck, title="Deck ordenado"):
    """
    Imprime las cartas de 'sorted_deck' en orden. Si hay
    cartas duplicadas consecutivas (mismo palo y rank),
    las agrupa e indica (xN) cuántas veces se repite.
    """
    print(f"\n-- {title} --")
    if not sorted_deck:
        print("No hay cartas para mostrar.")
        return
    
    # Iniciamos contadores
    prev_card = sorted_deck[0]
    count = 1
    
    for card in sorted_deck[1:]:
        # Si la carta actual es la misma (mismo suit y rank) que la anterior
        if (card.suit == prev_card.suit) and (card.rank == prev_card.rank):
            count += 1
        else:
            # Imprimimos la carta anterior con la cantidad acumulada
            if count > 1:
                print(f"{prev_card} (x{count})")
            else:
                print(f"{prev_card}")
            prev_card = card
            count = 1
    
    # Imprimir la última carta (o grupo) tras salir del bucle
    if count > 1:
        print(f"{prev_card} (x{count})")
    else:
        print(f"{prev_card}")


###############################################################################
#                 MAPEO DE PALOS Y RANGOS DE CARTAS                            #
###############################################################################
suits_order = {
    "corazones": 0,
    "tréboles": 1,
    "picas": 2,
    "diamantes": 3,
    "comodin": 4
}
ranks_order = {
    "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8,
    "9": 9, "10": 10, 'J': 11, 'Q': 12, 'K': 13, 'A': 1
}

###############################################################################
#                       CLASES CARD Y DECK                                     #
###############################################################################
class Card:
    """
    Representa una carta con 'suit' (palo) y 'rank' (rango).
    Ejemplos de suit: 'corazones', 'tréboles', 'picas', 'diamantes', 'comodin'.
    Ejemplos de rank: '2', '3', ..., '10', 'J', 'Q', 'K', 'A'.
    """
    def __init__(self, suit, rank):
        self.suit = suit
        self.rank = rank

    def __repr__(self):
        if self.suit == "comodin":
            return f"Comodín {self.rank}"
        return f"{self.rank} de {self.suit}"

    def key(self):
        """
        Retorna una tupla (ordenPalo, ordenRango) que indica
        cómo se ordenará la carta.
        """
        if self.suit == "comodin":
            return (suits_order[self.suit], self.rank)
        else:
            return (suits_order[self.suit], ranks_order[self.rank])

class Deck:
    """
    Representa una 'baraja' que contiene:
    - varios mazos (cada uno de 52 cartas de 4 palos)
    - más 2 comodines en total.
    """
    def __init__(self, numDecks=1):
        self.suits = ["corazones", "tréboles", "picas", "diamantes"]
        self.ranks = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
        self.deck = self.createDeck(numDecks)

    def createDeck(self, numDecks=1):
        """
        Crea 'numDecks' mazos de 52 cartas cada uno,
        luego agrega 2 comodines al final.
        Finalmente, mezcla todas las cartas.
        """
        deck = [Card(suit, rank) for rank in self.ranks for suit in self.suits] * numDecks
        # Agregamos 2 comodines extras (en total, no por cada mazo)
        deck.extend([Card("comodin", 1), Card("comodin", 2)] * numDecks) 
        
        random.shuffle(deck)
        return deck

###############################################################################
#                           FUNCIÓN PRINCIPAL                                  #
###############################################################################
if __name__ == "__main__":
    # =========================================================================
    #  1. Creamos ~4 millones de cartas
    # =========================================================================
    # 54 cartas por mazo, incluyendo 2 comodines
    num_decks = int(4_000_000 / 54)
    deck_obj = Deck(num_decks)
    
    total_cards = len(deck_obj.deck)
    print(f"Baraja con {total_cards} cartas.")

    # =========================================================================
    #  2. Copias: una para mergesort paralelo, otra para sorted secuencial
    # =========================================================================
    deck_for_parallel   = list(deck_obj.deck)
    deck_for_sequential = list(deck_obj.deck)

    # =========================================================================
    #  3. Profundidad recomendada y lectura del usuario
    # =========================================================================
    optimal_depth = get_optimal_max_depth()
    print(f"Profundidad óptima recomendada (según núcleos): {optimal_depth}")
    try:
        user_depth = int(input(f"Ingrese la profundidad de paralelismo (recomendado <= {optimal_depth}): "))
    except ValueError:
        user_depth = optimal_depth
    
    print("max_depth =", user_depth)

    # =========================================================================
    #  4. Ordenamiento paralelo
    # =========================================================================
    start_par = time.perf_counter()
    sorted_parallel = parallel_merge_sort(deck_for_parallel, key=card_sort_key, max_depth=user_depth)
    end_par = time.perf_counter()
    print(f"Orden paralelo tardó {end_par - start_par:.4f} s")

    # =========================================================================
    #  5. Ordenamiento secuencial
    # =========================================================================
    start_seq = time.perf_counter()
    sorted_sequential = sorted(deck_for_sequential, key=card_sort_key)
    end_seq = time.perf_counter()
    print(f"Orden secuencial tardó {end_seq - start_seq:.4f} s")
    # =========================================================================
    #  6. Baraja Ordenada
    # =========================================================================
    print_deck_with_duplicates(sorted_parallel)

 
