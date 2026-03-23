"""
Trie (Prefix Tree) - Autocompletado de búsquedas
=================================================
Referencia: Fredkin, E. (1960). "Trie Memory". Communications of the ACM.

Uso en el proyecto:
    Motor de autocompletado de búsquedas en la plataforma de streaming.
    Mientras el usuario escribe "aven...", el Trie devuelve en tiempo
    real sugerencias como ["Avengers", "Avengers: Endgame", "Avatar"].
    Las sugerencias se ordenan por popularidad (número de búsquedas).

Complejidad:
    - Inserción:  O(L)  donde L = longitud de la cadena
    - Búsqueda:   O(L)
    - Prefijos:   O(L + K) donde K = número de resultados
    - Espacio:    O(SIGMA * N * L) donde SIGMA = tamaño del alfabeto

Trade-offs vs Hash Table:
    + Búsquedas por prefijo nativas (imposible con hash)
    + Ordenamiento lexicográfico natural
    - Mayor uso de memoria que hash table para strings aleatorios
    - Más lento para búsqueda exacta que O(1) del hash
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import heapq


@dataclass
class TrieNode:
    children: dict[str, TrieNode] = field(default_factory=dict)
    is_end: bool = False
    frequency: int = 0      # Popularidad del término completo
    total_freq: int = 0     # Suma de frecuencias del subárbol (para ranking)


class Trie:
    """
    Trie con soporte de frecuencias para ranking de sugerencias.
    """

    def __init__(self):
        self.root = TrieNode()
        self.total_words = 0

    def insert(self, word: str, frequency: int = 1) -> None:
        """
        Inserta una palabra con su popularidad. O(L)
        Si la palabra ya existe, incrementa su frecuencia.
        """
        word = word.lower().strip()
        if not word:
            return

        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
            node.total_freq += frequency

        if not node.is_end:
            self.total_words += 1
        node.is_end = True
        node.frequency += frequency

    def search(self, word: str) -> int:
        """
        Busca una palabra exacta. Retorna su frecuencia o 0. O(L)
        """
        node = self._get_node(word.lower())
        return node.frequency if node and node.is_end else 0

    def starts_with(self, prefix: str) -> bool:
        """Verifica si alguna palabra empieza con el prefijo. O(L)"""
        return self._get_node(prefix.lower()) is not None

    def autocomplete(self, prefix: str, top_k: int = 5) -> list[tuple[str, int]]:
        """
        Retorna las top_k sugerencias para el prefijo dado,
        ordenadas por frecuencia descendente. O(L + N)

        Returns:
            Lista de (palabra, frecuencia) ordenada por popularidad
        """
        prefix = prefix.lower().strip()
        node = self._get_node(prefix)
        if node is None:
            return []

        results = []
        self._dfs(node, prefix, results)

        # Usar heap para obtener top_k eficientemente
        return heapq.nlargest(top_k, results, key=lambda x: x[1])

    def _dfs(self, node: TrieNode, current: str, results: list) -> None:
        """DFS para recolectar todas las palabras desde un nodo."""
        if node.is_end:
            results.append((current, node.frequency))
        for char, child in node.children.items():
            self._dfs(child, current + char, results)

    def _get_node(self, prefix: str) -> Optional[TrieNode]:
        """Navega hasta el nodo final del prefijo."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node

    def delete(self, word: str) -> bool:
        """Elimina una palabra del Trie. Retorna True si existía."""
        return self._delete_helper(self.root, word.lower(), 0)

    def _delete_helper(self, node: TrieNode, word: str, depth: int) -> bool:
        if depth == len(word):
            if not node.is_end:
                return False
            node.is_end = False
            node.frequency = 0
            self.total_words -= 1
            return len(node.children) == 0

        char = word[depth]
        if char not in node.children:
            return False

        should_delete = self._delete_helper(node.children[char], word, depth + 1)
        if should_delete:
            del node.children[char]
            return not node.is_end and len(node.children) == 0
        return False

    def __len__(self):
        return self.total_words

    def __repr__(self):
        return f"Trie(palabras={self.total_words})"


class SearchAutocomplete:
    """
    Sistema de autocompletado para la plataforma de streaming.
    Combina Trie con actualización dinámica de popularidad.
    """

    def __init__(self):
        self.trie = Trie()
        self.search_counts: dict[str, int] = {}

    def load_catalog(self, titles: list[str]) -> None:
        """Carga el catálogo de contenido (títulos de videos)."""
        for title in titles:
            self.trie.insert(title, frequency=1)

    def record_search(self, query: str) -> None:
        """Registra una búsqueda y actualiza la popularidad."""
        query = query.lower().strip()
        self.search_counts[query] = self.search_counts.get(query, 0) + 1
        self.trie.insert(query, frequency=1)

    def suggest(self, prefix: str, top_k: int = 5) -> list[str]:
        """Retorna sugerencias ordenadas por popularidad."""
        results = self.trie.autocomplete(prefix, top_k)
        return [word for word, _ in results]

    def suggest_with_counts(self, prefix: str, top_k: int = 5) -> list[tuple[str, int]]:
        """Retorna sugerencias con su frecuencia de búsqueda."""
        return self.trie.autocomplete(prefix, top_k)
