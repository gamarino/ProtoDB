import unittest
import uuid
from ..common import Atom, QueryPlan, DBCollections
from ..lists import List, ListQueryPlan  # Importamos las clases que queremos probar


class TestList(unittest.TestCase):
    def setUp(self):
        """
        Setup de datos inicial para los tests.
        Crea instancias iniciales de `List` y otros valores necesarios.
        """
        self.empty_list = List(empty=True)  # Lista vacía
        self.single_value = Atom()  # Valor sencillo para pruebas
        self.list_with_values = List(value=self.single_value)

        # Lista balanceada con nodos hijo
        self.list_balanced = List(
            value=Atom(),
            previous=List(value=Atom()),
            next=List(value=Atom())
        )

    def test_empty_list_initialization(self):
        """Prueba que una lista vacía esté correctamente inicializada."""
        self.assertTrue(self.empty_list.empty)
        self.assertEqual(self.empty_list.count, 0)
        self.assertEqual(self.empty_list.height, 0)

    def test_single_node_initialization(self):
        """Prueba que un nodo con un solo valor esté correctamente inicializado."""
        self.assertFalse(self.list_with_values.empty)
        self.assertEqual(self.list_with_values.count, 1)
        self.assertEqual(self.list_with_values.value, self.single_value)

    def test_balance_factor_for_balanced_tree(self):
        """Prueba para calcular el balance en un árbol balanceado."""
        balance = self.list_balanced._balance()
        self.assertEqual(balance, 0, "Balance factor debería ser 0 en un árbol completamente balanceado.")

    def test_inserting_at_empty_list(self):
        """Inserta en una lista vacía y verifica el valor."""
        new_list = self.empty_list.insert_at(0, Atom())
        self.assertEqual(new_list.count, 1, "La lista debería tener 1 elemento después de insertar.")
        self.assertFalse(new_list.empty)

    def test_get_element_at_offset(self):
        """Prueba para verificar la obtención de elementos en offsets específicos."""
        # Crear una lista más compleja para pruebas de offset
        complex_list = List(
            value=Atom(value="root"),
            previous=List(value=Atom(value="left")),
            next=List(value=Atom(value="right"))
        )

        # Verificar distintos offsets
        self.assertEqual(complex_list.get_at(0).value, "left")
        self.assertEqual(complex_list.get_at(1).value, "root")
        self.assertEqual(complex_list.get_at(2).value, "right")
        self.assertIsNone(complex_list.get_at(100), "Un offset fuera de rango debería devolver None.")

    def test_rebalancing_required(self):
        """
        Inserta y elimina elementos en la lista, causando desequilibrios.
        Verifica que la lista se reequilibre correctamente.
        """
        unbalanced_list = List(
            value=Atom("root"),
            previous=List(
                value=Atom("left"),
                previous=List(value=Atom("left-left"))
            )  # Subárbol izquierdo completamente desbalanceado
        )
        self.assertEqual(unbalanced_list._rebalance()._balance(), 0,
                         "El balance debe ser restaurado después del reequilibrio.")

    def test_as_iterable(self):
        """Prueba la conversión de la lista a un iterable."""
        # Crear una lista más compleja.
        tree_list = List(
            value="root",
            previous=List(value="left"),
            next=List(value="right")
        )

        iterable = list(tree_list.as_iterable())
        self.assertTrue("left" in iterable and "root" in iterable and "right" in iterable)

    def test_remove_value(self):
        """Prueba la eliminación de un nodo en una lista."""
        start_list = List(
            value=1,
            previous=List(value=2),
            next=List(value=3)
        )
        after_removal = start_list.remove_at(1)
        self.assertEqual(after_removal.get_at(0), 2, "El nodo raíz debe ser eliminado correctamente.")
        self.assertEqual(after_removal.get_at(1), 3, "El nodo raíz debe ser eliminado correctamente.")
        self.assertEqual(after_removal.count, 2, "El nodo raíz debe ser eliminado correctamente.")

    def test_insert_and_retrieve_values(self):
        """Inserta múltiples valores y asegúrate de que la lista se actualice correctamente."""
        test_list = List(empty=True)
        test_list = test_list.insert_at(0, "A")
        test_list = test_list.insert_at(1, "B")
        test_list = test_list.insert_at(2, "C")

        self.assertEqual(test_list.get_at(0), "A")
        self.assertEqual(test_list.get_at(1), "B")
        self.assertEqual(test_list.get_at(2), "C")

    def test_append_values(self):
        """Prueba append_last y append_first."""
        # Comenzar con una lista vacía
        new_list = self.empty_list.append_first("first")
        self.assertEqual(new_list.get_at(0), "first", "El elemento 'first' debería estar en la posición 0.")
        self.assertEqual(new_list.count, 1, "La lista debería tener exactamente 1 elemento.")

        new_list = new_list.append_last("last")
        self.assertEqual(new_list.get_at(-1), "last", "El elemento 'last' debería estar en la última posición.")
        self.assertEqual(new_list.count, 2, "La lista debería tener 2 elementos después del append.")

    def test_slice_operations(self):
        """Prueba de slicing en una lista."""
        test_list = List(empty=True)
        for i in range(5):
            test_list = test_list.insert_at(i, f"Element {i}")

        sliced_list = test_list.slice(1, 4)  # Obtener elementos 1, 2, 3 (por rango exclusivo de 4)
        self.assertEqual(sliced_list.count, 3, "El slicing debería devolver exactamente 3 elementos.")

    def test_query_plan_execution(self):
        """Prueba la ejecución de QueryPlan."""
        test_data = List(value=Atom("a"))
        query_plan = ListQueryPlan(base=test_data)

        results = list(query_plan.execute())
        self.assertTrue(len(results) > 0, "Query execution debería devolver resultados.")
